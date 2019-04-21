package org.example.pravega.client.batchclient.embeddedinproccluster;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;

@Slf4j
public class AuthBatchClient extends BatchClient {

    @Before
    @Override
    public void setup() throws Exception {
        this.isAuthEnabled = true;
        this.isTlsEnabled = false;
        super.setup();
    }
}
