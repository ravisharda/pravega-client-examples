package org.example.pravegaclientsamples;

import lombok.extern.slf4j.Slf4j;
import org.example.pravegaclientsamples.common.Reader;
import org.example.pravegaclientsamples.common.Writer;
import org.junit.Test;

import org.example.pravegaclientsamples.utilities.EnvironmentProperties;

import java.net.URI;

@Slf4j
public class BasicReaderWriterTests {

    @Test
    public void writeEventsThenReadAndPrintThem() {
        String controllerUri = EnvironmentProperties.defaultControllerUri();
        String scope = "org.example";
        String streamName = "testStream";
        String routingKey = "testRoutingKey";
        log.info("Controller Uri: {}", controllerUri);

        Writer writer = new Writer(scope, streamName, URI.create(controllerUri));

        String message1 = "message 1";
        writer.writeEvent(routingKey, message1);
        log.info(String.format("Done writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                message1, routingKey, scope, streamName));

        String message2 = "message 2";
        writer.writeEvent(routingKey, message2);
        log.info(String.format("Done writing message: '%s' with routing-key: '%s' to stream '%s / %s'%n",
                message2, routingKey, scope, streamName));

        Reader reader = new Reader(scope, streamName, URI.create(controllerUri));
        reader.readAndPrintAllEvents();
    }
}
