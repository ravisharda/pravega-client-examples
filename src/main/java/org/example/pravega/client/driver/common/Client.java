package org.example.pravega.client.driver.common;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.net.URI;

@Accessors(fluent = true) @Getter
public class Client {

    private final org.example.pravega.client.driver.common.Connection connection;

    public Client(String scope, String streamName, URI controllerURI) {
        this.connection = new Connection(scope, streamName, controllerURI);
    }

    public Client (String scope, String streamName, URI controllerURI, boolean tlsEnabled,
                   String trustStoreLocation) {

        this.connection = new Connection(scope, streamName, controllerURI,
                false, null, null,
                true, trustStoreLocation);
    }

    public Client(String scope, String streamName, URI controllerURI, boolean authEnabled,
                  String userName, String password) {
        if (authEnabled) {
            // ensure user name and passwords are not blank
        }
        this.connection = new Connection(scope, streamName, controllerURI,
                authEnabled, userName, password,
                false, null);
    }
}
