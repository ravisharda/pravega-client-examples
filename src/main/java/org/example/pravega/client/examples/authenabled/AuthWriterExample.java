package org.example.pravega.client.examples.authenabled;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.Cleanup;

import java.net.URI;

public class AuthWriterExample {

    public static void main(String... args) {
        String scope = "org.example.auth";
        String streamName = "stream1";
        int numSegments = 10;
        URI controllerURI = URI.create("tcp://localhost:9090");

        ClientConfig clientConfig = ClientConfig.builder()
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .controllerURI(controllerURI)
                .build();
        System.out.println("Done creating client config");

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        System.out.println("Created a stream manager");

        streamManager.createScope(scope);
        System.out.println("Created a scope: " + scope);

        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build());
        System.out.println("Created stream: " + streamName);

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, clientConfig);

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<String>(),
                EventWriterConfig.builder().build());
        System.out.println("Got a writer");

        writer.writeEvent("Hello-1");
        writer.writeEvent("Hello-2");
        System.out.println("Wrote data to the stream");
    }
}
