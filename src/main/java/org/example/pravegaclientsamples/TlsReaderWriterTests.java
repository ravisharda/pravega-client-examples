package org.example.pravegaclientsamples;

import lombok.extern.slf4j.Slf4j;
import org.example.pravegaclientsamples.common.Reader;
import org.example.pravegaclientsamples.common.Writer;
import org.example.pravegaclientsamples.utilities.EnvironmentProperties;
import org.example.pravegaclientsamples.utilities.FileUtils;
import org.junit.Test;

import java.net.URI;

@Slf4j
public class TlsReaderWriterTests {

    @Test
    public void writeEventsThenReadAndPrintThem() {
        String scope = "org.example";
        String streamName = "testStreamTls";
        String routingKey = "testRoutingKeyTls";

        String controllerUri = EnvironmentProperties.defaultControllerUri(true);
        log.info("Controller Uri: {}", controllerUri);

        String trustStoreFilePath = FileUtils.absolutePathOfFileInClasspath("cert.pem");

        // Write events to stream
        Writer writer = new Writer(scope, streamName, URI.create(controllerUri), trustStoreFilePath);

        String message1 = "message 1";
        writer.writeEvent(routingKey, message1);
        log.info("Done writing message '{}' with routing-key '{}' to stream '{} / {}'",
                message1, routingKey, scope, streamName);

        String message2 = "message 2";
        writer.writeEvent(routingKey, message2);
        log.info("Done writing message: '{}' with routing-key: '{}' to stream '{} / {}'%n",
                message2, routingKey, scope, streamName);

        // Now, read the events and print them to logs
        Reader reader = new Reader(scope, streamName, URI.create(controllerUri), trustStoreFilePath);
        reader.readAndPrintAllEvents();
        //log.info("Done reading all events");
    }
}
