package org.example.pravega.client.basicreadwrite.tlsandauthenabled;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;

public class SecureWriter {
    public static void main(String[] args) {
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(Constants.CONTROLLER_URI)
                .trustStore(Constants.TRUSTSTORE_PATH)
                .validateHostName(false)
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .build();

        // Everything below depicts the usual flow of writing events. All client-side security configuration is
        // done through the ClientConfig object as shown above.

        System.out.println("Done creating client config.");

        StreamManager streamManager = null;
        ClientFactory clientFactory = null;
        EventStreamWriter<String> writer = null;

        try {
            streamManager = StreamManager.create(clientConfig);
            System.out.println("Done creating a stream manager.");

            streamManager.createScope(Constants.SCOPE);
            System.out.println("Done creating a scope with the specified name: [" + Constants.SCOPE + "].");

            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(Constants.NO_OF_SEGMENTS))
                    .build();
            System.out.println("Done creating a stream configuration.");

            streamManager.createStream(Constants.SCOPE, Constants.STREAM_NAME, streamConfig);
            System.out.println("Done creating a stream with the specified name: [" + Constants.STREAM_NAME
                    + "] and stream configuration.");

            clientFactory = ClientFactory.withScope(Constants.SCOPE, clientConfig);
            System.out.println("Done creating a client factory with the specified scope and client config.");

            writer = EventStreamClientFactory.withScope(Constants.SCOPE, clientConfig)
                    .createEventWriter(Constants.STREAM_NAME,
                            new JavaSerializer<String>(),
                            EventWriterConfig.builder().build());
            System.out.println("Done creating a writer.");

            writer.writeEvent(Constants.MESSAGE).join();
            System.out.println("Done writing an event: [" + Constants.MESSAGE + "].");
        } finally {
            if (writer != null) writer.close();
            if (clientFactory != null) clientFactory.close();
            if (streamManager != null) streamManager.close();
        }
        System.err.println("All done with writing! Exiting...");
    }
}