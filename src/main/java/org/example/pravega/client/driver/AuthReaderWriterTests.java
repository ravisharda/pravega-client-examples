package org.example.pravega.client.driver;

import lombok.extern.slf4j.Slf4j;
import org.example.pravega.client.driver.common.Reader;
import org.example.pravega.client.driver.common.Writer;
import org.example.pravega.client.driver.utilities.EnvironmentProperties;
import org.junit.Test;

import java.net.URI;

@Slf4j
public class AuthReaderWriterTests {

    @Test
    public void writeWithoutProperCredentialsFails() {
        String controllerUri = EnvironmentProperties.defaultControllerUri();
        String scope = "org.example";
        String streamName = "testStreamAuth";
        String routingKey = "testRoutingKeyAuth";
        log.info("Controller Uri: {}", controllerUri);

        Writer writer = new Writer(scope, streamName, URI.create(controllerUri),
                "random", "random");
        writer.writeEvent(routingKey, "whatever");
    }

    @Test
    public void writeEventsThenReadAndPrintThem() {
        String controllerUri = EnvironmentProperties.defaultControllerUri();
        String scope = "org.example";
        String streamName = "testStreamAuth";
        String routingKey = "testRoutingKeyAuth";
        log.info("Controller Uri: {}", controllerUri);

        Writer writer = new Writer(scope, streamName, URI.create(controllerUri),
                "admin", "1111_aaaa");

        String message1 = "message 1";
        writer.writeEvent(routingKey, message1);
        log.info(String.format("Done writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                message1, routingKey, scope, streamName));

        String message2 = "message 2";
        writer.writeEvent(routingKey, message2);
        log.info(String.format("Done writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                message2, routingKey, scope, streamName));

        Reader reader = new Reader(scope, streamName, URI.create(controllerUri),
                "admin", "1111_aaaa");
        reader.readAndPrintAllEvents();
    }

    @Test
    public void readPreviousEventsAndPrintThem() {
        String controllerUri = EnvironmentProperties.defaultControllerUri();
        String scope = "org.example";
        String streamName = "testStreamAuth";
        String routingKey = "testRoutingKeyAuth";
        log.info("Controller Uri: {}", controllerUri);

        Reader reader = new Reader(scope, streamName, URI.create(controllerUri),
                "admin", "1111_aaaa");
        reader.readAndPrintAllEvents();
    }


}
