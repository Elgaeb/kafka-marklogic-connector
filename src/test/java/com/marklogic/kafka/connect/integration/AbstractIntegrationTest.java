package com.marklogic.kafka.connect.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.DeleteListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.kafka.connect.DefaultDatabaseClientConfigBuilder;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import org.junit.jupiter.api.AfterEach;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractIntegrationTest {
    private Map<String, String> originalConfiguration;
    private Map<String, Object> config;
    private DatabaseClient databaseClient;

    protected Map<String, String> getOriginalConfiguration() {
        return getOriginalConfiguration(null);
    }

    protected Map<String, String> getOriginalConfiguration(Map<String, String> additionalProperties, String... additionalDataSets) {
        if(this.originalConfiguration == null) {
            Properties props = new Properties();

            try (InputStream is = this.getClass().getResourceAsStream("/kafka.properties")) {
                props.load(is);
            } catch (Throwable t) {
            }

            for(String dataSet : additionalDataSets) {
                try (InputStream is = this.getClass().getResourceAsStream("/kafka-" + dataSet + ".properties")) {
                    props.load(is);
                } catch (Throwable t) {
                }
            }

            try (InputStream is = this.getClass().getResourceAsStream("/kafka-local.properties")) {
                props.load(is);
            } catch (Throwable t) {
            }

            Map<String, String> originalConfiguration = new HashMap<>();
            props.stringPropertyNames().forEach(propertyName -> originalConfiguration.put(propertyName, props.getProperty(propertyName)));

            if(additionalProperties != null) {
                additionalProperties.entrySet().forEach(entry -> originalConfiguration.put(entry.getKey(), entry.getValue()));
            }

            this.originalConfiguration = Collections.unmodifiableMap(originalConfiguration);
        }

        return this.originalConfiguration;
    }

    protected Map<String, Object> getConfiguration() {
        return this.getConfiguration(null);
    }

    protected Map<String, Object> getConfiguration(Map<String, String> additionalProperties) {
        if(this.config == null) {
            this.config = Collections.unmodifiableMap(MarkLogicSinkConfig.hydrate(this.getOriginalConfiguration(additionalProperties)));
        }

        return this.config;
    }

    protected Map<String, Object> readJsonDocument(String uri) throws IOException {
        DocumentManager mgr = jsonDocumentManager();

        InputStreamHandle handle = new InputStreamHandle();
        mgr.read(uri, handle);

        ObjectMapper mapper = jsonObjectMapper();
        return mapper.readValue(handle.get(), HashMap.class);
    }

    protected DatabaseClient databaseClient() {
        if(this.databaseClient == null) {
            DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(this.getConfiguration());
            this.databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);
        }
        return this.databaseClient;
    }

    protected DocumentManager jsonDocumentManager() {
        return databaseClient().newJSONDocumentManager();
    }

    protected ObjectMapper jsonObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        return mapper
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .setDateFormat(new StdDateFormat().withColonInTimeZone(true))
                .registerModule(new JavaTimeModule());
    }

    protected void deleteTestCollections() {
        DataMovementManager dmm = this.databaseClient().newDataMovementManager();
        QueryManager qm = this.databaseClient.newQueryManager();
        StructuredQueryBuilder sqb = qm.newStructuredQueryBuilder();

        Collection<String> collections = (Collection<String>) this.getConfiguration().getOrDefault(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, Collections.emptyList());
        if(!collections.isEmpty()) {
            StructuredQueryDefinition query = sqb.collection(collections.toArray(new String[collections.size()]));
            QueryBatcher batcher = dmm.newQueryBatcher(query);
            batcher
                    .withConsistentSnapshot()
                    .onUrisReady(new DeleteListener());
            dmm.startJob(batcher);
            batcher.awaitCompletion();
            dmm.stopJob(batcher);
        }
    }

    protected Map<String, Object> extractInstance(String schemaName, String tableName, Map<String, Object> document) {
        Map<String, Object> envelope = (Map<String, Object>) document.get("envelope");
        assertThat(envelope).isNotNull();

        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        assertThat(instance).isNotNull();

        Map<String, Object> schema = (Map<String, Object>) instance.get(schemaName);
        assertThat(instance).isNotNull();

        Map<String, Object> table = (Map<String, Object>) schema.get(tableName);
        assertThat(instance).isNotNull();

        return table;
    }

    //    @AfterEach
    public void cleanup() {
        deleteTestCollections();
    }

}
