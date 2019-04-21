package org.example.pravega.client.basicreadwrite.tlsandauthenabled;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Defines constants shared by classes in this package.
 */
public class Constants {
    static final String SCOPE = "io.pravega.clientsamples.secure";
    static final String STREAM_NAME = "mystream";
    static final URI CONTROLLER_URI = URI.create("tls://localhost:9090");
    static final int NO_OF_SEGMENTS = 1;
    static String READER_GROUP_NAME;

    static final String MESSAGE = "hello https world!";

    static String TRUSTSTORE_PATH;

    static {
        READER_GROUP_NAME = UUID.randomUUID().toString().replace("-", "");
        try {
            TRUSTSTORE_PATH = Paths.get(Constants.class.getClassLoader().getResource("cert.pem").toURI())
                    .toAbsolutePath().toString();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}