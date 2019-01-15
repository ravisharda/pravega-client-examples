package org.example.pravegaclientsamples.common;

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.net.URI;

@Accessors(fluent = true) @Getter
public class Connection {

    private final @NonNull String scope;
    private final @NonNull String streamName;

    private ClientConfig clientConfig;

    public Connection(@NonNull String scope, @NonNull String streamName, @NonNull URI controllerURI)
             {
        this(scope, streamName, controllerURI,
                false, null, null,
                false, null);
    }

    public Connection (@NonNull String scope, @NonNull String streamName, @NonNull URI controllerURI,
                       boolean isAuthEnabed, String username, String password,
                       boolean isTlsEnabled, String trustStoreLocation) {
        this.scope = scope;
        this.streamName = streamName;

        ClientConfig.ClientConfigBuilder clientConfigBuilder = ClientConfig.builder()
                .controllerURI(controllerURI);

        if (isAuthEnabed) {
            clientConfigBuilder.credentials((new DefaultCredentials(password, username)));
        }

        if (isTlsEnabled) {
            clientConfigBuilder.trustStore(trustStoreLocation).validateHostName(false);
        }

        this.clientConfig = clientConfigBuilder.build();
    }



}
