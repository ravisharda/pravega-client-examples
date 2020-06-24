package org.example.pravega.shared;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.example.pravega.shared.PathUtils.locationFromClasspath;

@Slf4j
public class StandaloneServerTlsConstants {

    private static final String CA_CERT_FILE_NAME = "ca-cert.crt";

    private static final String SERVER_CERT_FILE_NAME = "server-cert.crt";

    private static final String SERVER_KEY_FILE_NAME = "server-key.key";

    private static final String SERVER_KEYSTORE_FILE_NAME = "server.keystore.jks";

    private static final String SERVER_KEYSTORE_PWD_FILE_NAME = "server.keystore.jks.passwd";

    private static final String TRUSTSTORE_FILE_NAME = "client.truststore.jks";

    private static final String SERVER_PASSWD_FILE_NAME = "passwd";

    private static final String STANDALONE_SERVER_RESOURCES_DIR = "pravega/standalone/";

    public static final String CA_CERT_LOCATION = locationFromClasspath(STANDALONE_SERVER_RESOURCES_DIR + CA_CERT_FILE_NAME);

    public static final String SERVER_CERT_LOCATION = locationFromClasspath(STANDALONE_SERVER_RESOURCES_DIR + SERVER_CERT_FILE_NAME);

    public static final String SERVER_KEY_LOCATION = locationFromClasspath(STANDALONE_SERVER_RESOURCES_DIR + SERVER_KEY_FILE_NAME);

    public static final String SERVER_KEYSTORE_LOCATION = locationFromClasspath(STANDALONE_SERVER_RESOURCES_DIR+ SERVER_KEYSTORE_FILE_NAME);

    public static final String SERVER_KEYSTORE_PWD_LOCATION = locationFromClasspath(STANDALONE_SERVER_RESOURCES_DIR + SERVER_KEYSTORE_PWD_FILE_NAME);

    public static final String TRUSTSTORE_LOCATION = locationFromClasspath(STANDALONE_SERVER_RESOURCES_DIR + TRUSTSTORE_FILE_NAME);

    public static final String SERVER_PASSWD_LOCATION = locationFromClasspath(STANDALONE_SERVER_RESOURCES_DIR + SERVER_PASSWD_FILE_NAME);

    @SneakyThrows
    public static void main(String... args) {
        log.info("Ca cert: {}", CA_CERT_LOCATION);
        log.info("Server cert: {}", SERVER_CERT_LOCATION);
        log.info("Server key: {}", SERVER_KEY_LOCATION);
        log.info("Server keystore: {}", SERVER_KEYSTORE_LOCATION);
        log.info("Server keystore password: {}", SERVER_KEYSTORE_PWD_LOCATION);
        log.info("Server truststore: {}", TRUSTSTORE_LOCATION);
    }
}
