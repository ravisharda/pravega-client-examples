package org.example.inprocclustersamples;

import io.pravega.local.InProcPravegaCluster;
import lombok.extern.slf4j.Slf4j;

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
                .restServerPort(9091)
                .isInProcSegmentStore(true)
                .segmentStoreCount(1)
                .containerCount(4)
                .enableTls(false)
                .enableAuth(false)
                .build();

       inProcCluster.setControllerPorts(new int[]{9090});
       inProcCluster.setControllerPorts(new int[]{6060});

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
