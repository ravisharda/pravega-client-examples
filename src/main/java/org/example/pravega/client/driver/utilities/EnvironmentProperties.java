package org.example.pravega.client.driver.utilities;

import lombok.NonNull;

public class EnvironmentProperties {

    public static String defaultControllerUri() {
        return defaultControllerUri(false);
    }

    public static String defaultControllerUri(boolean tlsEnabled) {
        if (!tlsEnabled) {
            return controllerUri("tcp", "localhost", 9090);
        } else {
            return controllerUri("tls", "localhost", 9090);
        }
    }

    public static String controllerUri(@NonNull String protocol, @NonNull String controllerIp, int port) {
        return String.format("%s://%s:%d", protocol, controllerIp, port);
    }

    public static String defaultRestUri() {
        return restUri("http", "localhost", 9091);
    }

    public static String restUri(String protocol, String serverIp, int restPort) {
        return String.format("%s://%s:%d", protocol, serverIp, restPort);
    }
}
