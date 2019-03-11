package org.example.inprocclustersamples;

import io.pravega.local.InProcPravegaCluster;
import static org.example.pravegaclientsamples.utilities.FileUtils.*;

public class SecurityEnabledClusterExample {

    public static void main (String... args) {

        //System.out.println(absolutePathOfFileInClasspath("pravega/standalone/cert.pem"));
        InProcPravegaCluster inProcCluster = InProcPravegaCluster.builder()
                .isInProcZK(true)
                .zkUrl("localhost:" + 4000)
                .zkPort(4000)
                .isInMemStorage(true)
                .isInProcController(true)
                .controllerCount(1)
                .restServerPort(9091)
                .isInProcSegmentStore(true)
                .segmentStoreCount(1)
                .containerCount(4)
                .enableTls(true)
                .keyFile(absolutePathOfFileInClasspath("pravega/standalone/key.pem"))
                .certFile(absolutePathOfFileInClasspath("pravega/standalone/cert.pem"))
                .enableAuth(true)
                .userName("")
                .passwd("1111_aaaa")
                .passwdFile(absolutePathOfFileInClasspath("pravega/standalone/passwd"))
                .jksKeyFile(absolutePathOfFileInClasspath("pravega/standalone/standalone.keystore.jks"))
                .jksTrustFile(absolutePathOfFileInClasspath("pravega/standalone/standalone.truststore.jks"))
                .keyPasswordFile(absolutePathOfFileInClasspath("pravega/standalone/standalone.keystore.jks.passwd"))
                .build();
        //inProcPravegaCluster


    }
}
