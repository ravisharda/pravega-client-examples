package org.example.pravega.client.keyvaluetables;

import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.TableEntry;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.example.pravega.shared.StandaloneServerTlsConstants;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

@Slf4j
public class KeyValueTablesTests {

    private static final String TRUSTSTORE_PATH = StandaloneServerTlsConstants.CA_CERT_LOCATION;
    private static String CONTROLLER_HOST = "localhost";
    private static int PORT = 9090;

    @Test
    public void createTableAndAddAnEntryNoSecurity() {
        String scopeName = "testScope" + Math.random();
        String tableName = "testTable";
        String keyFamily = "profile";
        String key = "firstName";
        String value = "john";

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerUri(false))
                .build();

        createScope(scopeName, clientConfig);

        KeyValueTableManager keyValueTableManager = KeyValueTableManager.create(clientConfig);
        log.debug("Created KeyValueTableManager");

        boolean result = keyValueTableManager.createKeyValueTable(scopeName, tableName,
                KeyValueTableConfiguration.builder().partitionCount(1).build());
        log.debug("Created table {}", tableName);
        assertTrue(result);

        KeyValueTableFactory factory = KeyValueTableFactory.withScope(scopeName, clientConfig);
        KeyValueTable<String, String> table =
                factory.forKeyValueTable(tableName, new UTF8StringSerializer(), new UTF8StringSerializer(),
                        KeyValueTableClientConfiguration.builder().build());
        log.debug("Created table {}", tableName);

        table.put(keyFamily, key, value).join();
        log.debug("Added an entry to the table", tableName);

        TableEntry<String, String> entry = table.get(keyFamily, key).join();
        log.debug("Retrieved an entry from the table", tableName);

        assertNotNull(entry);
        assertEquals(value, entry.getValue());
    }

    @Test
    public void createTableAndAddAnEntryWithSecurity() {
        String scopeName = "testScope" + Math.random();
        String tableName = "testTable";
        String keyFamily = "profile";
        String key = "firstName";
        String value = "john";

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerUri(false))
                .controllerURI(controllerUri(true))
                .trustStore(TRUSTSTORE_PATH)
                .validateHostName(false)
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .build();

        createScope(scopeName, clientConfig);

        KeyValueTableManager keyValueTableManager = KeyValueTableManager.create(clientConfig);
        log.debug("Created KeyValueTableManager");

        boolean result = keyValueTableManager.createKeyValueTable(scopeName, tableName,
                KeyValueTableConfiguration.builder().partitionCount(1).build());
        log.debug("Created table {}", tableName);
        assertTrue(result);

        KeyValueTableFactory factory = KeyValueTableFactory.withScope(scopeName, clientConfig);
        KeyValueTable<String, String> table =
                factory.forKeyValueTable(tableName, new UTF8StringSerializer(), new UTF8StringSerializer(),
                        KeyValueTableClientConfiguration.builder().build());
        log.debug("Created table {}", tableName);

        table.put(keyFamily, key, value).join();
        log.debug("Added an entry to the table", tableName);

        TableEntry<String, String> entry = table.get(keyFamily, key).join();
        log.debug("Retrieved an entry from the table", tableName);

        assertNotNull(entry);
        assertEquals(value, entry.getValue());
    }

    @Test
    public void createTableAndAddAnEntry() {
        String scopeName = "testScope" + Math.random();
        String tableName = "testTable";
        String keyFamily = "profile";
        String key = "firstName";
        String value = "john";

        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(controllerUri(true))
                .trustStore(TRUSTSTORE_PATH)
                .validateHostName(false)
                .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                .build();

        createScope(scopeName, clientConfig);

        KeyValueTableManager keyValueTableManager = KeyValueTableManager.create(clientConfig);
        log.debug("Created KeyValueTableManager");

        boolean result = keyValueTableManager.createKeyValueTable(scopeName, tableName,
                        KeyValueTableConfiguration.builder().partitionCount(1).build());
        log.debug("Created table {}", tableName);
        assertTrue(result);

        KeyValueTableFactory factory = KeyValueTableFactory.withScope(scopeName, clientConfig);
        KeyValueTable<String, String> table =
                factory.forKeyValueTable(tableName, new UTF8StringSerializer(), new UTF8StringSerializer(),
                KeyValueTableClientConfiguration.builder().build());
        log.debug("Created table {}", tableName);

        table.put(keyFamily, key, value).join();
        log.debug("Added an entry to the table", tableName);

        TableEntry<String, String> entry = table.get(keyFamily, key).join();
        log.debug("Retrieved an entry from the table", tableName);

        assertNotNull(entry);
        assertEquals(value, entry.getValue());
    }

    private boolean createScope(String scopeName, ClientConfig clientConfig) {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        log.debug("Created a stream manager");

        // Create scope
        boolean result = streamManager.createScope(scopeName);
        log.debug("Created a scope {}, with result {}", scopeName, result);
        return result;
    }

    private static URI controllerUri(boolean tlsEnabled) {
        return URI.create(String.format("%s://%s:%d", tlsEnabled ? "tls" : "tcp", CONTROLLER_HOST, PORT));
    }

    public static void main(String... args) {
        System.out.println(KeyValueTablesTests.controllerUri(false));
        System.out.println(KeyValueTablesTests.controllerUri(true));
    }
}
