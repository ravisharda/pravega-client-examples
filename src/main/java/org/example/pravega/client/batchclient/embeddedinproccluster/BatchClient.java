package org.example.pravega.client.batchclient.embeddedinproccluster;

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.local.InProcPravegaCluster;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static junit.framework.TestCase.assertTrue;
import static org.example.pravega.common.FileUtils.absolutePathOfFileInClasspath;
import static org.junit.Assert.assertEquals;

@Slf4j
public class BatchClient {

    //private static final int READ_TIMEOUT = 1000;

    boolean isAuthEnabled = false;
    boolean isTlsEnabled = false;

    InProcPravegaCluster inProcCluster = null;

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private final Random random = RandomFactory.create();
    private static final String DATA_OF_SIZE_30 = "data of size 30"; // data length = 22 bytes , header = 8 bytes


    @Before
    public void setup() throws Exception {
        InProcPravegaCluster.InProcPravegaClusterBuilder builder = InProcPravegaCluster.builder()
                .isInProcZK(true)
                .zkUrl("localhost:" + 4000)
                .zkPort(4000)
                .isInMemStorage(true)
                .isInProcController(true)
                .controllerCount(1)
                .enableRestServer(true)
                .restServerPort(9091)
                .isInProcSegmentStore(true)
                .segmentStoreCount(1)
                .containerCount(4);

        if (isTlsEnabled) {
            builder.enableTls(true)
                    .keyFile(absolutePathOfFileInClasspath("pravega/standalone/key.pem"))
                    .certFile(absolutePathOfFileInClasspath("pravega/standalone/cert.pem"))
                    .jksKeyFile(absolutePathOfFileInClasspath("pravega/standalone/standalone.keystore.jks"))
                    .jksTrustFile(absolutePathOfFileInClasspath("pravega/standalone/standalone.truststore.jks"))
                    .keyPasswordFile(absolutePathOfFileInClasspath("pravega/standalone/standalone.keystore.jks.passwd"));
        }

        if (isAuthEnabled) {
            builder.enableAuth(false)
                    .userName("admin")
                    .passwd("1111_aaaa")
                    .passwdFile(absolutePathOfFileInClasspath("pravega/standalone/passwd"));
        }

        inProcCluster = builder.build();

        inProcCluster.setControllerPorts(new int[]{9090});
        inProcCluster.setSegmentStorePorts(new int[]{6000});

        log.info("Starting in-proc Cluster...");
        inProcCluster.start();
        log.info("Done starting in-proc Cluster.");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutdown Hook is running...");
                try {
                    inProcCluster.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        log.info("Application terminating...");
    }

    @After
    public void tearDown() throws Exception {
        if (inProcCluster != null) {
            inProcCluster.close();
        }
    }

    protected ClientConfig prepareClientConfig() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder()
                .controllerURI(controllerUri());

        if (isTlsEnabled) {
            builder.trustStore(Utils.pathToFileInClasspath("cert.pem"))
                    .validateHostName(false);
        }
        if (isAuthEnabled) {
            builder.credentials(new DefaultCredentials("1111_aaaa", "admin"));
        }
        ClientConfig result = builder.build();
        log.debug("clientConfig: " + result);
        return result;
    }

    protected URI controllerUri() {
        if (isTlsEnabled) {
            return URI.create("tls://localhost:9090");
        } else {
            return URI.create("tcp://localhost:9090");
        }
    }

    @Test
    public void batchClientReadSegments() throws ExecutionException, InterruptedException {
        String scopeName = "basicTestScope";
        String streamName = "basicTestStream";

        ClientConfig clientConfig = prepareClientConfig();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        log.info("Created a stream manager");

        // Create the scope
        streamManager.createScope(scopeName);
        log.info("Created a scope [{}]", scopeName);

        // Create a stream with 1 segment
        streamManager.createStream(scopeName, streamName,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        log.info("Created a stream with name [{}]", streamName);


        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scopeName, clientConfig);
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<String>(),
                EventWriterConfig.builder().build());

        // write 3 30 byte events to the test stream with 1 segment.
        write30ByteEvents(3, writer);
        log.info("Wrote 3 events, while the stream had 1 segment.");

        // Create the gRPC client proxy, that we'll use for scaling up the streams
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(
                ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());
        Stream stream = Stream.of(scopeName, streamName);

        /* scale the stream up to 3 segments and then write events. */

        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        boolean scaleResult = controller.scaleStream(stream, Collections.singletonList(0L), map, executor)
                .getFuture()
                .get();
        assertTrue("Scale up operation result", scaleResult);
        log.info("Done scaling up the stream to 3 segments. Scale result was {}", scaleResult);

        write30ByteEvents(3, writer);
        log.info("Wrote 3 more events.");

        /* Scale the stream down to 2 segments, and then write some more events */

        map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);

        scaleResult = controller.scaleStream(stream,
                Arrays.asList(computeSegmentId(1, 1),
                        computeSegmentId(2, 1),
                        computeSegmentId(3, 1)), map, executor).getFuture().get();
        log.info("Done scaling down the stream to 2 segments. Scale result was {}", scaleResult);
        assertTrue("Scale down operation result", scaleResult);
        write30ByteEvents(3, writer);
        log.info("Wrote 3 more events.");

        /* Now, using a batch client to read back the events */

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(scopeName, clientConfig);

        // List out all the segments in the stream.
        ArrayList<SegmentRange> segments = Lists.newArrayList(batchClient.getSegments(
                stream, null, null).getIterator());
        assertEquals("Expected number of segments", 6, segments.size());

        // Batch read all events from stream.
        List<String> batchEventList = new ArrayList<>();
        segments.forEach(segInfo -> {
            @Cleanup
            SegmentIterator<String> segmentIterator = batchClient.readSegment(segInfo, new JavaSerializer<>());
            batchEventList.addAll(Lists.newArrayList(segmentIterator));
        });
        assertEquals("Event count", 9, batchEventList.size());

        // Read from a given offset.
        Segment seg0 = new Segment(scopeName, streamName, 0);
        SegmentRange seg0Info = SegmentRangeImpl.builder().segment(seg0).startOffset(60).endOffset(90).build();

        @Cleanup
        SegmentIterator<String> seg0Iterator = batchClient.readSegment(seg0Info, new JavaSerializer<>());
        ArrayList<String> dataAtOffset = Lists.newArrayList(seg0Iterator);
        assertEquals(1, dataAtOffset.size());
        assertEquals(DATA_OF_SIZE_30, dataAtOffset.get(0));
    }

    private void write30ByteEvents(int numberOfEvents, EventStreamWriter<String> writer) {
        Supplier<String> routingKeyGenerator = () -> String.valueOf(random.nextInt());
        IntStream.range(0, numberOfEvents).forEach(v -> writer.writeEvent(routingKeyGenerator.get(),
                DATA_OF_SIZE_30).join());
    }
}
