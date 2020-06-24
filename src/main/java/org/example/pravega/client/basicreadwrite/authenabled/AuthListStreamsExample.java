package org.example.pravega.client.basicreadwrite.authenabled;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;

public class AuthListStreamsExample {

    private static URI prepareControllerUri() {
        return URI.create("tcp://localhost:9090");
    }

    private static ClientConfig newAdminClientConfig() {
        return ClientConfig.builder()
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .controllerURI(prepareControllerUri())
                .build();
    }

    @Test
    public void listStreamsListsAllStreamsForPrivilegedUser() {
        ClientConfig clientConfig = this.newAdminClientConfig();
        String scopeName = "listfiltering";

        StreamManager streamManager = null;
        try {
            streamManager = StreamManager.create(clientConfig);
            assertNotNull(streamManager);

            boolean isScopeCreated = streamManager.createScope(scopeName);
            assertTrue("Failed to create scope", isScopeCreated);

            boolean isStreamCreated = streamManager.createStream(scopeName, "stream1",
                    StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
            Assert.assertTrue("Failed to create the 'stream1'", isStreamCreated);

            isStreamCreated = streamManager.createStream(scopeName, "stream2",
                    StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
            Assert.assertTrue("Failed to create the 'stream2'", isStreamCreated);

            Iterator<Stream> streamsIter = streamManager.listStreams(scopeName);
            Set<Stream> streams = new HashSet<>();
            streamsIter.forEachRemaining(s -> streams.add(s));
            assertSame(2, streams.size());
        } finally {
            if (streamManager != null) {
                streamManager.close();
            }
        }
    }
}
