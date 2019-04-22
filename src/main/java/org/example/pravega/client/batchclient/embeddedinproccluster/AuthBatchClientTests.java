package org.example.pravega.client.batchclient.embeddedinproccluster;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class AuthBatchClientTests extends BatchClientTests {

    @Before
    @Override
    public void setup() throws Exception {
        this.isAuthEnabled = true;
        this.isTlsEnabled = false;
        super.setup();
    }

    @Override
    @Test
    public void batchClientReadSegments() throws Exception {
        super.batchClientReadSegments();
    }

    @Override
    @Test
    public void batchClientStreamCuts() {
        super.batchClientStreamCuts();
    }
}
