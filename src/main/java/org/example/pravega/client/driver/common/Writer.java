package org.example.pravega.client.driver.common;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

public class Writer extends Client {

    public Writer(String scope, String streamName, URI controllerURI) {
        super(scope, streamName, controllerURI);
    }

    public Writer (String scope, String streamName, URI controllerURI, String trustStoreLocation) {
        super(scope, streamName, controllerURI, true, trustStoreLocation);
    }

    public Writer(String scope, String streamName, URI controllerURI, String userName, String password) {
        super(scope, streamName, controllerURI, true, userName, password);
    }

    public void writeEvent(String routingKey, String message) {

        Connection conn = connection();

        // Stream manager is used to manage streams and reader groups.
        StreamManager streamManager = StreamManager.create(conn.clientConfig());

        final boolean scopeIsNew = streamManager.createScope(conn.scope());

        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final boolean streamIsNew = streamManager.createStream(conn.scope(),
                conn.streamName(), streamConfig);

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(conn.scope(),
                conn.clientConfig());
             EventStreamWriter<String> writer = clientFactory.createEventWriter(conn.streamName(),
                     new JavaSerializer<String>(),
                     EventWriterConfig.builder().build())) {

            final CompletableFuture writeFuture = writer.writeEvent(routingKey, message);
        }
    }
}
