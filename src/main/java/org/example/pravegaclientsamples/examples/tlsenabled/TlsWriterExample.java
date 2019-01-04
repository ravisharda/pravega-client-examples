package org.example.pravegaclientsamples.examples.tlsenabled;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;

import java.net.URI;

public class TlsWriterExample {

    static {
        // SslVerificationDisabler.disableSslVerification();
    }

    private static final int READER_TIMEOUT_MS = 2000;

    public static void main(String... args) {
        String scope = "tls";
        String streamName = "stream1";
        int numSegments = 10;
        URI controllerURI = URI.create("tls://localhost:9090");


        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerURI)
                .trustStore("/home/rsharda/my-pravega-apps/src/main/resources/cert.pem")
                .validateHostName(false)
                .build();
        System.out.println("Done creating client config");

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

        try (ClientFactory clientFactory = ClientFactory.withScope(scope, clientConfig);
            EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                     new JavaSerializer<String>(),
                     EventWriterConfig.builder().build())) {
            System.out.println("Got a writer");

            writer.writeEvent("Hello-1");
            writer.writeEvent("Hello-2");
            System.out.println("Wrote data to the stream");
        }


        /*
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        log.info("Created writer for stream: " + streamName);

        writer.writeEvent("hello").get();

        */


    }
}
