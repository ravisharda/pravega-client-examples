package org.example.pravegaclientsamples.examples.securitydisabled;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.example.pravegaclientsamples.utilities.Utils;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@Slf4j
public class ReaderWriterExample {

    @Test
    public void testWriteAndReadEventWhenTlsIsDisabled() throws ReinitializationRequiredException {
        String scope = "org.example.insecure" + Utils.randomWithRange(1, 100);
        String streamName = "stream1";

        int numSegments = 1;
        URI controllerURI = URI.create("tcp://localhost:9090");

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerURI)
                .build();
        log.info("Done creating client config");

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
        String readEvent1 = reader.readNextEvent(2000).getEvent();
        log.info("Done reading event [{}]", readEvent1);

        String readEvent2 = reader.readNextEvent(2000).getEvent();
        log.info("Done reading event [{}]", readEvent2);

        assertEquals(writeEvent1, readEvent1);
        assertEquals(writeEvent2, readEvent2);
    }
}
