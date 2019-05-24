package org.example.pravega.client.basicreadwrite.tlsenabled;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;
import org.example.pravega.common.FileUtils;

import java.net.URI;
import java.util.UUID;

public class TlsReaderExample {

    private static final int READER_TIMEOUT_MS = 2000;

    public static void main(String... args) {

        String scope = "tls";
        String streamName = "stream1";
        int numSegments = 10;

        URI controllerURI = URI.create("tls://localhost:9090");

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerURI)
                .trustStore(FileUtils.absolutePathOfFileInClasspath("cert.pem"))
                .validateHostName(false)
                .build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        System.out.println("Created a stream manager");

        // A Stream is created in the context of a Scope; the Scope acts as a namespace mechanism so
        // that different sets of Streams can be categorized for some purpose.
        streamManager.createScope(scope);
        System.out.println("Created a scope: " + scope);

        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        System.out.println("Created stream: " + streamName);

        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .build();

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
        readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, clientConfig);

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader",
                readerGroup,
                new JavaSerializer<String>(),
                ReaderConfig.builder().build());
        System.out.format("Reading all the events from %s/%s%n", scope, streamName);

        EventRead<String> event = null;
        do {
            try {
                event = reader.readNextEvent(READER_TIMEOUT_MS);
                if (event.getEvent() != null) {
                    System.out.format("Read event '%s'%n", event.getEvent());
                }
            } catch (ReinitializationRequiredException e) {
                // there are certain circumstances where the reader needs to be reinitialized
                e.printStackTrace();
            }
        } while (event.getEvent() != null);
        System.out.format("No more events from %s/%s%n", scope, streamName);
    }
}
