package org.example.pravega.client.batchclient.embeddedinproccluster;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.local.InProcPravegaCluster;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import static org.example.pravega.common.FileUtils.absolutePathOfFileInClasspath;
import static org.junit.Assert.assertEquals;
import static org.example.pravega.client.batchclient.embeddedinproccluster.BatchClientTestHelper.*;

@Slf4j
public class BatchClientTests {

    boolean isAuthEnabled = false;
    boolean isTlsEnabled = false;

    InProcPravegaCluster inProcCluster = null;

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4,
            "executor");

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
            builder.enableAuth(true)
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
            builder.trustStore(absolutePathOfFileInClasspath("cert.pem"))
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
    public void batchClientReadSegments() throws Exception {
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

        // Create the gRPC client proxy, that we'll use for scaling the streams
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(
                ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());
        Stream stream = Stream.of(scopeName, streamName);

        addEventsToStream(writer, scopeName, streamName, controller, executor);

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

    // Not working yet, so ignored for now.
    @Ignore
    @Test
    public void batchClientStreamCuts() {
        String scopeName = "testscope";
        String streamName = "teststream";
        String readerGroupName = "rg";
        int readerGroupParallelism = 4;
        int totalEvents = readerGroupParallelism * 10;
        int offsetEvents = readerGroupParallelism * 20;
        int numSegments = 1;
        int batchIterations = 4;

        final Stream stream = Stream.of(scopeName, streamName);
        final ClientConfig clientConfig = prepareClientConfig();
        log.info("Done creating client config");

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        log.info("Done creating a stream manager.");

        streamManager.createScope(scopeName);
        log.info("Done creating a scope with the specified name: [" + scopeName + "].");

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build();
        log.info("Done creating a stream configuration.");

        streamManager.createStream(scopeName, streamName, streamConfig);
        log.info("Done creating a stream with the specified name: [{}] and stream configuration [{}]"
                , streamName, streamConfig);

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        log.info("Done creating connectionFactory");

        // Create the gRPC client proxy
        ControllerImpl controller = new ControllerImpl(
                ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());
        log.info("Done creating controller proxy ControllerImpl");

        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scopeName, controller);
        log.info("Done creating ClientFactoryImpl");

        @Cleanup
        BatchClientFactory batchClientFactory = BatchClientFactory.withScope(scopeName, clientConfig);
        log.info("Done creating batchClientFactory");

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scopeName, controllerUri());

        String streamFQN = scopeName + "/" + streamName;

        groupManager.createReaderGroup(readerGroupName,
                ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                        .stream(streamFQN)
                        .build());
        log.info("Done creating a ReaderGroup with name {} and for stream {}", readerGroupName, streamFQN);

        ReaderGroup readerGroup = groupManager.getReaderGroup(readerGroupName);
        log.info("Done creating batchClientFactory");

        // Write events to the Stream.
        writeEvents(clientFactory, streamName, totalEvents);
        log.info("Done writing events");

        // Instantiate readers to consume from Stream up to truncatedEvents.
        List<CompletableFuture<Integer>> futures =
                readEventFutures(clientFactory, readerGroupName, readerGroupParallelism, offsetEvents);
        Futures.allOf(futures).join();

        // Create a stream cut on the specified offset position.
        Checkpoint cp = readerGroup.initiateCheckpoint("batchClientCheckpoint", executor).join();
        StreamCut streamCut = cp.asImpl().getPositions().values().iterator().next();

        // Instantiate the batch client and assert it provides correct stream info.
        log.debug("Creating batch client.");
        StreamInfo streamInfo = streamManager.getStreamInfo(scopeName, stream.getStreamName());
        log.debug("Validating stream metadata fields.");
        assertEquals("Expected Stream name: ", streamName, streamInfo.getStreamName());
        assertEquals("Expected Scope name: ", scopeName, streamInfo.getScope());

        // Test that we can read events from parallel segments from an offset onwards.
        log.debug("Reading events from stream cut onwards in parallel.");
        List<SegmentRange> ranges = Lists.newArrayList(
                batchClientFactory.getSegments(stream, streamCut, StreamCut.UNBOUNDED).getIterator());
        assertEquals("Expected events read: ", totalEvents - offsetEvents, readFromRanges(ranges, batchClientFactory));

        // Emulate the behavior of Hadoop client: i) Get tail of Stream, ii) Read from current point until tail, iii) repeat.
        log.debug("Reading in batch iterations.");
        StreamCut currentTailStreamCut = streamManager.getStreamInfo(scopeName, stream.getStreamName()).getTailStreamCut();
        int readEvents = 0;
        for (int i = 0; i < batchIterations; i++) {
            writeEvents(clientFactory, streamName, totalEvents);

            // Read all the existing events in parallel segments from the previous tail to the current one.
            ranges = Lists.newArrayList(batchClientFactory.getSegments(stream, currentTailStreamCut, StreamCut.UNBOUNDED).getIterator());
            assertEquals("Expected number of segments: ", readerGroupParallelism, ranges.size());
            readEvents += readFromRanges(ranges, batchClientFactory);
            log.debug("Events read in parallel so far: {}.", readEvents);
            currentTailStreamCut = streamManager.getStreamInfo(scopeName, stream.getStreamName()).getTailStreamCut();
        }

        assertEquals("Expected events read: .", totalEvents * batchIterations, readEvents);

        // Truncate the stream in first place.
        log.debug("Truncating stream at event {}.", offsetEvents);
        Assert.assertTrue(controller.truncateStream(scopeName, streamName, streamCut).join());

        // Test the batch client when we select to start reading a Stream from a truncation point.
        StreamCut initialPosition = streamManager.getStreamInfo(scopeName, stream.getStreamName()).getHeadStreamCut();
        List<SegmentRange> newRanges = Lists.newArrayList(batchClientFactory.getSegments(stream, initialPosition, StreamCut.UNBOUNDED).getIterator());
        assertEquals("Expected events read: ", (totalEvents - offsetEvents) + totalEvents * batchIterations,
                readFromRanges(newRanges, batchClientFactory));
        log.debug("Events correctly read from Stream: simple batch client test passed.");
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testBatchClientWithStreamTruncation() throws Exception {
        String scopeName = "testScope";
        String streamName = "testStream";
        Stream stream = Stream.of(scopeName, streamName);

        ClientConfig clientConfig = prepareClientConfig();
        StreamManager streamManager = StreamManager.create(clientConfig);

        // Create the gRPC client proxy, that we'll use for scaling the streams
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(
                ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());

        createTestStreamWithEvents(clientConfig, scopeName, streamName, controller, executor);

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(scopeName, clientConfig);

        // 1. Create a StreamCut after 2 events(offset = 2 * 30 = 60).
        StreamCut streamCut60L = new StreamCutImpl(stream,
                ImmutableMap.of(new Segment(scopeName, streamName, 0), 60L));

        // 2. Truncate stream.
        Assert.assertTrue("truncate stream",
                controller.truncateStream(scopeName, streamName, streamCut60L).get());

        // 3a. Fetch Segments using StreamCut.UNBOUNDED>
        ArrayList<SegmentRange> segmentsPostTruncation1 = Lists.newArrayList(
                batchClient.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());

        // 3b. Fetch Segments using getStreamInfo() api.
        StreamInfo streamInfo = streamManager.getStreamInfo(scopeName, streamName);
        ArrayList<SegmentRange> segmentsPostTruncation2 =
                Lists.newArrayList(batchClient.getSegments(stream, streamInfo.getHeadStreamCut(),
                        streamInfo.getTailStreamCut()).getIterator());

        // Validate results.
        validateSegmentCountAndEventCount(batchClient, segmentsPostTruncation1);
        validateSegmentCountAndEventCount(batchClient, segmentsPostTruncation2);
    }


    // Simplified version of batchClientReadSegments() test. TODO: keep one of them.
    @Ignore
    @Test
    public void test() throws ExecutionException, InterruptedException {
        String scopeName = "tscopeName";
        String streamName = "tscopeName";
        Stream stream = Stream.of(scopeName, streamName);
        Serializer<String> serializer = new JavaSerializer<String>();

        ClientConfig clientConfig = prepareClientConfig();


        // Create the gRPC client proxy, that we'll use for scaling the streams
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(
                ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());

        createTestStreamWithEvents(clientConfig, scopeName, streamName, controller, executor);

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(scopeName,
               clientConfig);

        // List out all the segments in the stream.
        ArrayList<SegmentRange> segments = Lists.newArrayList(batchClient.getSegments(
                stream, null, null).getIterator());

        assertEquals("Expected number of segments", 6, segments.size());

        // Batch read all events from stream.
        List<String> batchEventList = new ArrayList<>();
        segments.forEach(segInfo -> {
            @Cleanup
            SegmentIterator<String> segmentIterator = batchClient.readSegment(segInfo, serializer);
            batchEventList.addAll(Lists.newArrayList(segmentIterator));
        });
        assertEquals("Event count", 9, batchEventList.size());

        // Read from a given offset.
        Segment seg0 = new Segment(scopeName, streamName, 0);
        SegmentRange seg0Info = SegmentRangeImpl.builder().segment(seg0).startOffset(60).endOffset(90).build();

        @Cleanup
        SegmentIterator<String> seg0Iterator = batchClient.readSegment(seg0Info, serializer);
        ArrayList<String> dataAtOffset = Lists.newArrayList(seg0Iterator);
        assertEquals(1, dataAtOffset.size());
        assertEquals(DATA_OF_SIZE_30, dataAtOffset.get(0));
    }
}
