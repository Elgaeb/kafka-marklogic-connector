package com.marklogic.kafka.connect.integration;

import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import com.marklogic.kafka.connect.sink.MarkLogicSinkTask;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class PerformanceTest extends AbstractIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(PerformanceTest.class);

    private Schema keySchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int32().build())
            .build();

    private Schema valueSchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int32().build())
            .field("foreignTable1Id", SchemaBuilder.int32().build())
            .field("foreignTable2Id", SchemaBuilder.int32().build())
            .field("value1", SchemaBuilder.string().build())
            .field("value2", SchemaBuilder.string().build())
            .field("value3", SchemaBuilder.string().build())
            .field("value4", SchemaBuilder.string().build())
            .field("value5", SchemaBuilder.string().build())
            .build();

    private SecureRandom random = new SecureRandom();

    protected SinkRecord generateRecord(String topic) {
        int id = random.nextInt();
        int foreignTable1Id = random.nextInt();
        int foreignTable2Id = random.nextInt();

        String value1 = UUID.randomUUID().toString();
        String value2 = UUID.randomUUID().toString();
        String value3 = UUID.randomUUID().toString();
        String value4 = UUID.randomUUID().toString();
        String value5 = UUID.randomUUID().toString();

        Struct key = new Struct(keySchema);
        key.put("id", id);

        Struct value = new Struct(valueSchema);
        value.put("id", id);
        value.put("foreignTable1Id", foreignTable1Id);
        value.put("foreignTable2Id", foreignTable2Id);
        value.put("value1", value1);
        value.put("value2", value2);
        value.put("value3", value3);
        value.put("value4", value4);
        value.put("value5", value5);

        return new SinkRecord(topic, 0, keySchema, key, valueSchema, value, 0);
    }

    @Test
    public void testPerformance() {
        List<SinkRecord> records = new LinkedList<>();

        int count = 100000;
        Integer threadCount = 16;
        Integer batchSize = 50;

        for(int i = 0; i < count; i++) {
            records.add(generateRecord("schemaname-tablename"));
        }

        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put(MarkLogicSinkConfig.DMSDK_BATCH_SIZE, batchSize.toString());
        additionalProperties.put(MarkLogicSinkConfig.DMSDK_THREAD_COUNT, threadCount.toString());
        Map<String, String> config = getOriginalConfiguration(additionalProperties);

        MarkLogicSinkTask task = new MarkLogicSinkTask();
        task.start(config);

        Instant startPut = Instant.now();
        task.put(records);
        task.flushAndWait(Collections.emptyMap());
        Instant endPut = Instant.now();

        Duration dur = Duration.between(startPut, endPut);
        double seconds = dur.getSeconds();
        double nanos = dur.getNano();

        double totalSeconds = seconds + (nanos / 1000000000.0);
        double rate = (double)count / totalSeconds;

        task.stop();

        logger.info("Put {} records with batch size {} and thread count {} in {} seconds at {} documents/second.", count, batchSize, threadCount, totalSeconds, rate);
    }
}
