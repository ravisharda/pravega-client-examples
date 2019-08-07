package org.example.pravega.common;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Credentials;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ScopeAndStreamsCreator {

    private String scopeName;
    StreamManager streamManager = null;

    public ScopeAndStreamsCreator(String scopeName, ClientConfig clientConfig) {
        Preconditions.checkNotNull(clientConfig);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(scopeName));
        this.scopeName = scopeName;
        this.streamManager = StreamManager.create(clientConfig);
        log.debug("Done creating a stream manager.");

        streamManager.createScope(this.scopeName);
        log.debug("Done creating a scope with the specified name: [{}]", this.scopeName);
    }

    public void addAStreamWithFixedScalingPolicy(int numSegments, String streamName) {
        Preconditions.checkArgument(numSegments > 0);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments))
                .build();
        log.debug("Done creating a stream configuration.");

        streamManager.createStream(this.scopeName, streamName, streamConfig);
        log.debug("Done creating a stream with the specified name: [{}] and stream configuration.", streamName);

    }

    public void close() {
        if (streamManager != null) {
            streamManager.close();
        }
    }


}
