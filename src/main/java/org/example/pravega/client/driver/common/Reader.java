package org.example.pravega.client.driver.common;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.UUID;

@Slf4j
public class Reader extends Client {

    private static final int READER_TIMEOUT_MS = 2000;

    public Reader(String scope, String streamName, URI controllerURI) {
        super(scope, streamName, controllerURI);
    }

    public Reader (String scope, String streamName, URI controllerURI, String trustStoreLocation) {
        super(scope, streamName, controllerURI, true, trustStoreLocation);
    }

    public Reader(String scope, String streamName, URI controllerURI, String userName, String password) {
        super(scope, streamName, controllerURI, false, userName, password);
    }

    public void readAndPrintAllEvents () {

        Connection conn = this.connection();

        StreamManager streamManager = StreamManager.create(conn.clientConfig());

        final boolean scopeIsNew = streamManager.createScope(conn.scope());
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        final boolean streamIsNew = streamManager.createStream(conn.scope(), conn.streamName(), streamConfig);

        final String readerGroup = UUID.randomUUID().toString().replace("-", "");
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(conn.scope(), conn.streamName()))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(conn.scope(),
                conn.clientConfig())) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        try (ClientFactory clientFactory = ClientFactory.withScope(conn.scope(), conn.clientConfig());
             EventStreamReader<String> reader = clientFactory.createReader("reader",
                     readerGroup,
                     new JavaSerializer<String>(),
                     ReaderConfig.builder().build())) {
            log.info("Reading all events from '{}/{}'", conn.scope(), conn.streamName());

            EventRead<String> event = null;
            do {
                try {
                    event = reader.readNextEvent(READER_TIMEOUT_MS);
                    if (event.getEvent() != null) {
                        log.info("Read event '{}", event.getEvent());
                    }
                } catch (ReinitializationRequiredException e) {
                    e.printStackTrace();
                }
            } while (event.getEvent() != null);
            log.info("No more events from '{}/{}'", conn.scope(), conn.streamName());
        }
    }
}
