package org.example.pravegaclientsamples.examples.tlsenabled;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.example.pravegaclientsamples.utilities.FileUtils;
import org.example.pravegaclientsamples.utilities.Utils;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class TlsReaderWriterExample {

    @Test(timeout = 50000)
    public void testWriteAndReadEventWhenConfigurationIsProper() throws ExecutionException,
            InterruptedException, ReinitializationRequiredException {

        String scope = "TlsTestScope" + Utils.randomWithRange(1, 1000);
        String streamName = "TlsTestStream";
        int numSegments = 10;
        String message = "Test event over TLS channel";
        URI controllerURI = URI.create("tls://localhost:9090");

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerURI)
                .trustStore(FileUtils.absolutePathOfFileInClasspath("cert.pem"))
                .validateHostName(false)
                .build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        boolean isScopeCreated = streamManager.createScope(scope);
        //assertTrue("Failed to create scope", isScopeCreated);

        boolean isStreamCreated = streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        Assert.assertTrue("Failed to create the stream ", isStreamCreated);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);

        // Write an event to the stream.

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        writer.writeEvent(message).get();
        log.info("Done writing message '{}' to stream '{} / {}'", message, scope, streamName);

        // Now, read the event from the stream.

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
        String readMessage = reader.readNextEvent(10000).getEvent();
        log.info("Done reading event [{}]", readMessage);

        assertEquals(message, readMessage);
    }


}
