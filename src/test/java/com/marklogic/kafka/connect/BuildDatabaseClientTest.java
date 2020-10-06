package com.marklogic.kafka.connect;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class BuildDatabaseClientTest {

    @Test
    public void testSimpleSslConfiguration() {
        DatabaseClient databaseClient;

        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSinkConfig.CONNECTION_PORT, 8002);
        config.put(MarkLogicSinkConfig.CONNECTION_USERNAME, "admin");
        config.put(MarkLogicSinkConfig.CONNECTION_PASSWORD, "admin");
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "DIGEST");
        config.put(MarkLogicSinkConfig.CONNECTION_TYPE, "DIRECT");
        config.put(MarkLogicSinkConfig.SSL_CONNECTION_TYPE, "simple");

        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(config);
        databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);

        Assertions.assertNotNull(databaseClient);
    }

    @Test
    public void testDefaultSslConfiguration() {
        DatabaseClient databaseClient;

        Map<String, Object> config = new HashMap<>();
        config.put(MarkLogicSinkConfig.CONNECTION_HOST, "localhost");
        config.put(MarkLogicSinkConfig.CONNECTION_PORT, 8002);
        config.put(MarkLogicSinkConfig.CONNECTION_USERNAME, "admin");
        config.put(MarkLogicSinkConfig.CONNECTION_PASSWORD, "admin");
        config.put(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE, "DIGEST");
        config.put(MarkLogicSinkConfig.CONNECTION_TYPE, "DIRECT");
        config.put(MarkLogicSinkConfig.SSL_CONNECTION_TYPE, "default");

        DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(config);
        databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);

        Assertions.assertNotNull(databaseClient);
    }
}
