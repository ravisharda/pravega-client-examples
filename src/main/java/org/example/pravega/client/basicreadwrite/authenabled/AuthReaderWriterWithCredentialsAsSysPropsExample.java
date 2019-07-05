package org.example.pravega.client.basicreadwrite.authenabled;

import io.pravega.client.ClientConfig;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AuthReaderWriterWithCredentialsAsSysPropsExample extends AuthReaderWriterExample {

    // We use this to ensure that the tests that depend on system properties are run one at a time, in order to avoid
    // one test causing side effects in another.
    Lock sequential = new ReentrantLock();

    @Override
    protected ClientConfig prepareValidClientConfig() {
        return ClientConfig.builder()
                .controllerURI(prepareControllerUri())
                .build();
    }

    @Test
    @Override
    public void testWriteAndReadEvent() {
        // Using a lock to prevent concurrent execution of tests that set system properties.
        sequential.lock();

        // Set auth params via Java system properties
        setClientAuthParams("admin", "1111_aaaa");
        //setClientAuthSystemProperties("StaticTokenAuthHandler", "static-token"); // a custom AuthHandler implementation

        try {
            super.testWriteAndReadEvent();
            unsetClientAuthProperties();
        } finally {
            sequential.unlock();
        }
    }

    private void setClientAuthParams(String userName, String password) {
        // Prepare the token to be used for basic authentication
        String plainToken = userName + ":" + password;
        String base66EncodedToken = Base64.getEncoder().encodeToString(plainToken.getBytes(StandardCharsets.UTF_8));
        setClientAuthSystemProperties("Basic", base66EncodedToken);
    }

    private void setClientAuthSystemProperties(String scheme, String token) {
        System.setProperty("pravega.client.auth.method", scheme);
        System.setProperty("pravega.client.auth.token", token);
    }

    private void unsetClientAuthProperties()  {
        System.clearProperty("pravega.client.auth.method");
        System.clearProperty("pravega.client.auth.token");
    }
}
