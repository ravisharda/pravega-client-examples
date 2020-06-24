package org.example.pravega.inproccluster;

import io.pravega.local.InProcPravegaCluster;
import lombok.extern.slf4j.Slf4j;
import org.example.pravega.shared.StandaloneServerTlsConstants;

@Slf4j
public class NoSecurityInProcClusterExample {
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
                .enableTls(false)
                .enableAuth(false)
                .keyFile(StandaloneServerTlsConstants.SERVER_KEY_LOCATION)
                .certFile(StandaloneServerTlsConstants.SERVER_CERT_LOCATION)
                .userName("")
                .passwd("1111_aaaa")
                .passwdFile(StandaloneServerTlsConstants.SERVER_PASSWD_LOCATION)
                .jksKeyFile(StandaloneServerTlsConstants.SERVER_CERT_LOCATION)
                .jksTrustFile(StandaloneServerTlsConstants.TRUSTSTORE_LOCATION)
                .keyPasswordFile(StandaloneServerTlsConstants.SERVER_KEYSTORE_PWD_LOCATION)
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
