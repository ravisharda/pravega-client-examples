package org.example.pravega.client.basicreadwrite.json;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.DefaultCredentials;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;

@Slf4j
public class JSONWriter {

    public final String scope;
    public final String streamName;
    public final URI controllerURI;

    public JSONWriter(String scope, String streamName, URI controllerURI) {
        this.scope = scope;
        this.streamName = streamName;
        this.controllerURI = controllerURI;
    }

    public void run(String routingKey) throws InterruptedException {

        String streamName = "json-stream";

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(URI.create(controllerURI.toString()))
                //.credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .build();

        StreamManager streamManager = StreamManager.create(clientConfig);
        // create scope if not exists. This wont work when we try to create scope in nautilus. We need to use other methods to create scope on nautilus.
        streamManager.createScope(scope);
        StreamConfiguration streamConfig = StreamConfiguration.builder().build();
        streamManager.createStream(scope, streamName, streamConfig);

        // Create client config

        // Create EventStreamClientFactory
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        // Create event writer
        EventStreamWriter<JsonNode> writer = clientFactory.createEventWriter(
                streamName,
                new JsonNodeSerializer(),
                EventWriterConfig.builder().build());
        // same data write every 1 sec
        while (true) {
            ObjectNode data = createJSONData();
            writer.writeEvent(routingKey, data);
            Thread.sleep(1000);
        }
    }

    // Create a JSON data for testing purpose
    public static ObjectNode createJSONData() {
        ObjectNode message = null;
        try {
            String data = "{\"id\":" + Math.random() + ",\"name\":\"My Corp\",\"building\":3,\"location\":\"India\"}";
            // Deserialize the JSON message.
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(data);
            message = (ObjectNode) jsonNode;
            log.info("@@@@@@@@@@@@@ DATA >>>  " + message.toString());
            return message;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return message;
    }

    public static void main(String[] args) {
        final String scope = "test-scope2";
        final String streamName = "test-stream";
        final String routingKey = "test";
        final URI controllerURI = URI.create("tcp://10.247.118.51:9090");
                //URI.create("tcp://localhost:9090");
        JSONWriter ew = new JSONWriter(scope, streamName, controllerURI);
        try {
            ew.run(routingKey);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
