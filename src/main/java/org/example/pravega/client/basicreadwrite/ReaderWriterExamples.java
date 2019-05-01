package org.example.pravega.client.basicreadwrite;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.example.pravega.client.basicreadwrite.tlsandauthenabled.Constants;
import org.example.pravega.client.driver.utilities.Utils;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@Slf4j
public class ReaderWriterExamples {
    private static final String CONTROLLER_HOST = "localhost";
    private static final String TRUSTSTORE_PATH = "/path/to/ca-or-server-certificate";

    @Test
    public void writeAndReadEventWithTlsAndAuthEnabled() {
        String scope = "org.example.tlsandauth" + Utils.randomWithRange(1, 100);
        String streamName = "stream1";

        int numSegments = 1;
        URI controllerURI = controlerUri(true);

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerURI)
                .trustStore(TRUSTSTORE_PATH)
                .validateHostName(false)
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .build();
        log.info("Done creating client config");

        writeThenRead(scope, streamName, numSegments, clientConfig);
    }

    @Test
    public void testWriteAndReadEventWhenTlsIsDisabled() {
        String scope = "org.example.insecure" + Utils.randomWithRange(1, 100);
        String streamName = "stream1";

        int numSegments = 1;
        URI controllerURI = controlerUri(false);

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerURI)
                .build();
        log.info("Done creating client config");

        writeThenRead(scope, streamName, numSegments, clientConfig);
    }

    private void writeThenRead(String scopeName, String streamName, int numSegments, ClientConfig clientConfig) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        log.info("Created a stream manager");

        streamManager.createScope(scopeName);
        log.info("Created a scope [{}]", scopeName);

        streamManager.createStream(scopeName, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        log.info("Created a stream with name [{}]", streamName);

        @Cleanup
        EventStreamWriter<String> writer =
                EventStreamClientFactory.withScope(scopeName, clientConfig)
                .createEventWriter(streamName,
                        new JavaSerializer<String>(),
                        EventWriterConfig.builder().build());
        log.info("Got a writer");

        String writeEvent1 = "This is event 1";
        writer.writeEvent(writeEvent1);
        log.info("Done writing event [{}]", writeEvent1);

        String writeEvent2 = "This is event 2";
        writer.writeEvent(writeEvent2);
        log.info("Done writing event [{}]", writeEvent2);

        // Now, read back the events from the stream.

        String readerGroupName = UUID.randomUUID().toString().replace("-", "");
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scopeName, streamName))
                .disableAutomaticCheckpoints()
                .build();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scopeName, clientConfig);
        readerGroupManager.createReaderGroup(readerGroupName, readerGroupConfig);

        @Cleanup
        EventStreamReader<String> reader = EventStreamClientFactory.withScope(scopeName, clientConfig)
                .createReader("readerId", readerGroupName,
                        new JavaSerializer<String>(), ReaderConfig.builder().build());

        // Keeping the read timeout large so that there is ample time for reading the event even in
        // case of abnormal delays in test environments.
        String readEvent1 = null;
        String readEvent2 = null;
        try {
            readEvent1 = reader.readNextEvent(2000).getEvent();
            log.info("Done reading event [{}]", readEvent1);

            readEvent2 = reader.readNextEvent(2000).getEvent();
            log.info("Done reading event [{}]", readEvent2);
        } catch (ReinitializationRequiredException e) {
            throw new RuntimeException(e);
        }
        assertEquals(writeEvent1, readEvent1);
        assertEquals(writeEvent2, readEvent2);
    }

    private void writeThenreadUsingDeprecatedApi(String scope, String streamName, int numSegments, ClientConfig clientConfig) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        log.info("Created a stream manager");

        streamManager.createScope(scope);
        log.info("Created a scope [{}]", scope);

        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        log.info("Created a stream with name [{}]", streamName);

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, clientConfig);

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        log.info("Got a writer");

        String writeEvent1 = "This is event 1";
        writer.writeEvent(writeEvent1);
        log.info("Done writing event [{}]", writeEvent1);

        String writeEvent2 = "This is event 2";
        writer.writeEvent(writeEvent2);
        log.info("Done writing event [{}]", writeEvent2);

        // Now, read the events from the stream.

        String readerGroup = UUID.randomUUID().toString().replace("-", "");
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .disableAutomaticCheckpoints()
                .build();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(
                "readerId", readerGroup,
                new JavaSerializer<String>(), ReaderConfig.builder().build());

        // Keeping the read timeout large so that there is ample time for reading the event even in
        // case of abnormal delays in test environments.
        String readEvent1 = null;
        String readEvent2 = null;
        try {
            readEvent1 = reader.readNextEvent(2000).getEvent();
            log.info("Done reading event [{}]", readEvent1);

            readEvent2 = reader.readNextEvent(2000).getEvent();
            log.info("Done reading event [{}]", readEvent2);
        } catch (ReinitializationRequiredException e) {
            throw new RuntimeException(e);
        }
        assertEquals(writeEvent1, readEvent1);
        assertEquals(writeEvent2, readEvent2);
    }

    private URI controlerUri(boolean isTlsEnabled) {
        String uri = null;
        if (isTlsEnabled) {
            uri = String.format("tls://%s:9090", CONTROLLER_HOST);
        } else {
            uri = String.format("tcp://%s:9090", CONTROLLER_HOST);
        }
        return URI.create(uri);
    }
}
