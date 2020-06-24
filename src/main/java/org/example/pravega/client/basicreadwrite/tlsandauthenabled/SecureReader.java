package org.example.pravega.client.basicreadwrite.tlsandauthenabled;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;

public class SecureReader {

    public static void main(String[] args) throws ReinitializationRequiredException {

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(Constants.CONTROLLER_URI)
                .trustStore(Constants.TRUSTSTORE_PATH)
                .validateHostName(false)
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .build();
        System.out.println("Done creating a client config.");

        // Everything below depicts the usual flow of reading events. All client-side security configuration is
        // done through the ClientConfig object as shown above.

        EventStreamClientFactory clientFactory = null;
        ReaderGroupManager readerGroupManager = null;
        EventStreamReader<String> reader = null;
        try {
            ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                    .stream(Stream.of(Constants.SCOPE, Constants.STREAM_NAME))
                    .disableAutomaticCheckpoints()
                    .build();
            System.out.println("Done creating a reader group config with specified scope: [" +
                    Constants.SCOPE +"] and stream name: [" + Constants.STREAM_NAME + "].");

            readerGroupManager = ReaderGroupManager.withScope(Constants.SCOPE, clientConfig);
            readerGroupManager.createReaderGroup(Constants.READER_GROUP_NAME, readerGroupConfig);
            System.out.println("Done creating a reader group with specified name  and config.");

            clientFactory = EventStreamClientFactory.withScope(Constants.SCOPE, clientConfig);
            System.out.println("Done creating a client factory with the specified scope and client config.");

            reader = EventStreamClientFactory.withScope(Constants.SCOPE, clientConfig)
                    .createReader("readerId", Constants.READER_GROUP_NAME,
                            new JavaSerializer<String>(), ReaderConfig.builder().build());

            String readMessage = reader.readNextEvent(2000).getEvent();
            System.out.println("Done reading an event: [" + readMessage + "].");

        } finally {
            if (reader != null) reader.close();
            if (clientFactory != null) clientFactory.close();
            if (readerGroupManager != null) readerGroupManager.close();
        }
        System.err.println("All done with reading! Exiting...");
    }
}