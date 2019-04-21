package org.example.pravega.inproccluster;

import io.pravega.local.InProcPravegaCluster;
import lombok.extern.slf4j.Slf4j;

import static org.example.pravega.common.FileUtils.*;

@Slf4j
public class SecurityEnabledClusterExample {

    public static void main (String... args) throws Exception {

        InProcPravegaCluster inProcCluster = InProcPravegaCluster.builder()
                .isInProcZK(true)
                .zkUrl("localhost:" + 4000)
                .zkPort(4000)
                .isInMemStorage(true)
                .isInProcController(true)
                .controllerCount(1)
                .enableRestServer(true)
                .restServerPort(9091)
                .isInProcSegmentStore(true)
                .segmentStoreCount(1)
                .containerCount(4)
                .enableTls(true)
                .enableAuth(true)
                .keyFile(absolutePathOfFileInClasspath("pravega/standalone/key.pem"))
                .certFile(absolutePathOfFileInClasspath("pravega/standalone/cert.pem"))
                .userName("admin")
                .passwd("1111_aaaa")
                .passwdFile(absolutePathOfFileInClasspath("pravega/standalone/passwd"))
                .jksKeyFile(absolutePathOfFileInClasspath("pravega/standalone/standalone.keystore.jks"))
                .jksTrustFile(absolutePathOfFileInClasspath("pravega/standalone/standalone.truststore.jks"))
                .keyPasswordFile(absolutePathOfFileInClasspath("pravega/standalone/standalone.keystore.jks.passwd"))
                .build();

        inProcCluster.setControllerPorts(new int[]{9090});
        inProcCluster.setSegmentStorePorts(new int[]{6000});

        log.info("Starting in-proc Cluster...");
        inProcCluster.start();
        log.info("Done starting in-proc Cluster.");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Shutdown Hook is running...");
                try {
                    inProcCluster.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        log.info("Application terminating...");
    }
}
