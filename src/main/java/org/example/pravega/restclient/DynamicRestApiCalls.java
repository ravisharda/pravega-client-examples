package org.example.pravega.restclient;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertTrue;

@Slf4j
public class DynamicRestApiCalls {

    @Test
    public void listScopes() {
        Client client = ClientBuilder.newClient();
        String responseAsString = null;

        // Listing scopes (POST /v1/scopes)
        WebTarget target = client.target("http://localhost:9091").path("v1").path("scopes");
        try (Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get()) {

            if (response.getStatus() != 200) {
                throw new RuntimeException("Received unexpected HTTP status " + response.getStatus());
            }
            responseAsString = response.readEntity(String.class);
        }
        log.debug("Received response: {}", responseAsString);

        // Response will be look like this: {"scopes":[{"scopeName":"_system"}]}
        assertTrue(responseAsString.contains("\"scopeName\":\"_system\""));
    }

    @Test
    public void addScope() {
        Client client = ClientBuilder.newClient();
        String requestBody = "{\"scopeName\" : \"myScope\"}";
        String responseAsString = null;

        // Creating a scope (// POST /v1/scopes {"scope" : "<scopeName>"})
        WebTarget target = client.target("http://localhost:9091").path("v1").path("scopes");
        try (Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                .post(Entity.json(requestBody))) {

            if (response.getStatus() != HttpStatus.SC_CREATED) {
                throw new RuntimeException("Received unexpected HTTP status " + response.getStatus());
            }
            responseAsString = response.readEntity(String.class);
        }
        log.debug("Received response: {}", responseAsString);

        // Response will be look like this: {"scopes":[{"scopeName":"_system"}]}
         assertTrue(responseAsString.contains("\"scopeName\":\"myScope\""));
    }
}
