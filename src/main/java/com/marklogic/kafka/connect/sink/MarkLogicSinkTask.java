package com.marklogic.kafka.connect.sink;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.DefaultConfiguredDatabaseClientFactory;
import com.marklogic.kafka.connect.DefaultDatabaseClientConfigBuilder;
import com.marklogic.kafka.connect.sink.recordconverter.SinkRecordConverter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Performs the actual work associated with ingesting new documents into MarkLogic based on data received via the
 * "put" method.
 */
public class MarkLogicSinkTask extends SinkTask {

	private static final Logger logger = LoggerFactory.getLogger(MarkLogicSinkTask.class);

	private DatabaseClient databaseClient;
	private DataMovementManager dataMovementManager;
	private WriteBatcher writeBatcher;
	private SinkRecordConverter sinkRecordConverter;

	private Set<String> currentBatch = new HashSet<>();
	private int batchSize;

	@Override
	public void start(final Map<String, String> originalConfig) {
		logger.info("Starting");
		final Map<String, Object> config = MarkLogicSinkConfig.hydrate(originalConfig);
		config.keySet().forEach(key -> logger.info("Config: {} : {}", key, config.get(key)));

		try {
			Class converterClass = (Class) config.get(MarkLogicSinkConfig.DOCUMENT_CONVERTER);
			this.sinkRecordConverter = (SinkRecordConverter) converterClass.getDeclaredConstructor(Map.class).newInstance(config);
		} catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
			throw new RuntimeException(e);
		}

		DatabaseClientConfig databaseClientConfig = new DefaultDatabaseClientConfigBuilder().buildDatabaseClientConfig(config);
		databaseClient = new DefaultConfiguredDatabaseClientFactory().newDatabaseClient(databaseClientConfig);

		dataMovementManager = databaseClient.newDataMovementManager();

		this.batchSize = (Integer) config.get(MarkLogicSinkConfig.DMSDK_BATCH_SIZE);
		writeBatcher = dataMovementManager.newWriteBatcher()
				.withBatchSize(this.batchSize)
				.withThreadCount((Integer) config.get(MarkLogicSinkConfig.DMSDK_THREAD_COUNT));

		ServerTransform transform = buildServerTransform(config);
		if (transform != null) {
			writeBatcher.withTransform(transform);
		}

		final String flowName = (String) config.get(MarkLogicSinkConfig.DATAHUB_FLOW_NAME);
		if (flowName != null && flowName.trim().length() > 0) {
			writeBatcher.onBatchSuccess(buildSuccessListener(flowName, config, databaseClientConfig));
		}

		dataMovementManager.startJob(writeBatcher);

		logger.info("Started");
	}

	/**
	 * This is all specific to Kafka, as it involves reading inputs from the Kafka config map and then using them to
	 * construct the reusable RunFlowWriteBatchListener.
	 *
	 * @param flowName
	 * @param kafkaConfig
	 * @param databaseClientConfig
	 */
	protected RunFlowWriteBatchListener buildSuccessListener(String flowName, Map<String, Object> kafkaConfig, DatabaseClientConfig databaseClientConfig) {
		String logMessage = String.format("After ingesting a batch, will run flow '%s'", flowName);
		final String flowSteps = (String) kafkaConfig.get(MarkLogicSinkConfig.DATAHUB_FLOW_STEPS);
		List<String> steps = null;
		if (flowSteps != null && flowSteps.trim().length() > 0) {
			steps = Arrays.asList(flowSteps.split(","));
			logMessage += String.format(" with steps '%s' constrained to the URIs in that batch", steps.toString());
		}
		logger.info(logMessage);

		RunFlowWriteBatchListener listener = new RunFlowWriteBatchListener(flowName, steps, databaseClientConfig);
		if (kafkaConfig.containsKey(MarkLogicSinkConfig.DATAHUB_FLOW_LOG_RESPONSE)) {
			listener.setLogResponse((Boolean) kafkaConfig.get(MarkLogicSinkConfig.DATAHUB_FLOW_LOG_RESPONSE));
		}
		return listener;
	}

	/**
	 * Builds a REST ServerTransform object based on the DMSDK parameters in the given config. If no transform name
	 * is configured, then null will be returned.
	 *
	 * @param config - The complete configuration object including any transform parameters.
	 * @return - The ServerTransform that will operate on each record, or null
	 */
	protected ServerTransform buildServerTransform(final Map<String, Object> config) {
		String transform = (String) config.get(MarkLogicSinkConfig.DMSDK_TRANSFORM);
		if (transform != null && transform.trim().length() > 0) {
			ServerTransform t = new ServerTransform(transform);
			String params = (String) config.get(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS);
			if (params != null && params.trim().length() > 0) {
				String delimiter = (String) config.get(MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER);
				if (delimiter != null && delimiter.trim().length() > 0) {
					String[] tokens = params.split(delimiter);
					for (int i = 0; i < tokens.length; i += 2) {
						if (i + 1 >= tokens.length) {
							throw new IllegalArgumentException(String.format("The value of the %s property does not have an even number of " +
								"parameter names and values; property value: %s", MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS, params));
						}
						t.addParameter(tokens[i], tokens[i + 1]);
					}
				} else {
					logger.warn(String.format("Unable to apply transform parameters to transform: %s; please set the " +
						"delimiter via the %s property", transform, MarkLogicSinkConfig.DMSDK_TRANSFORM_PARAMS_DELIMITER));
				}
			}
			return t;
		}
		return null;
	}

	@Override
	public void stop() {
		logger.info("Stopping");
		try {
			try {
				if (writeBatcher != null) {
					writeBatcher.flushAndWait();
					dataMovementManager.stopJob(writeBatcher);
				}
			} finally {
				dataMovementManager.release();
			}
		} finally {
			if (databaseClient != null) {
				databaseClient.release();
			}
		}

		logger.info("Stopped");
	}

	protected void addUpdate(DocumentWriteOperation writeOperation, Integer partition, long offset) {
		String uri = writeOperation.getUri();

		if(this.currentBatch.contains(uri)) {
			// updating a document that's already in this batch. Need to flush.
			logger.info("Detected uri {} in existing batch. Flushing. Partition: {}, Offset: {}", uri, partition, offset);
			writeBatcher.flushAsync();
			this.currentBatch.clear();
		}

		writeBatcher.add(writeOperation);

		this.currentBatch.add(uri);
		if(this.currentBatch.size() == this.batchSize) {
			// TODO: There has to be a better way...
			this.currentBatch.clear();
		}
	}

	protected void addDeletes(Set<String> uris) {
		if(uris != null && uris.size() > 0) {
			if (this.currentBatch.stream().anyMatch(uris::contains)) {
				logger.info("Detected uri in existing batch. Flushing before delete.");
				writeBatcher.flushAsync();
				this.currentBatch.clear();
			}

			GenericDocumentManager documentManager = this.databaseClient.newDocumentManager();
			String[] urisArray = uris.toArray(new String[uris.size()]);
			documentManager.delete(urisArray);
		}
	}

	/**
	 * This is doing all the work of writing to MarkLogic, which includes calling flushAsync on the WriteBatcher.
	 * Alternatively, could move the flushAsync call to an overridden flush() method. Kafka defaults to flushing every
	 * 60000ms - this can be configured via the offset.flush.interval.ms property.
	 * <p>
	 * Because this is calling flushAsync, the batch size won't come into play unless the incoming collection has a
	 * size equal to or greater than the batch size.
	 *
	 * @param records - The records retrieved from Kafka
	 */
	@Override
	public void put(final Collection<SinkRecord> records) {
		if (records.isEmpty()) {
			return;
		}

		records.forEach(record -> {
			if (record == null) {
				logger.warn("Skipping null record object.");
			} else {
				if (logger.isDebugEnabled()) {
					logger.debug("Processing record value {} in topic {}", record.value(), record.topic());
				}
				if (record.value() != null) {
					UpdateOperation updateOperation = sinkRecordConverter.convert(record);
					this.addDeletes(updateOperation.getDeletes());
					updateOperation.getWrites().forEach(writeOperation -> this.addUpdate(writeOperation, updateOperation.getPartition(), updateOperation.getOffset()));
				} else {
					logger.warn("Skipping record with null value - possibly a 'tombstone' message.");
				}
			}
		});

//		if (writeBatcher != null) {
//			writeBatcher.flushAsync();
//		} else {
//			logger.warn("writeBatcher is null - ignore this is you are running unit tests, otherwise this is a problem.");
//		}
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		super.flush(currentOffsets);
		if (writeBatcher != null) {
			writeBatcher.flushAsync();
		} else {
			logger.warn("writeBatcher is null - ignore this is you are running unit tests, otherwise this is a problem.");
		}
	}

	public void flushAndWait(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		super.flush(currentOffsets);
		if (writeBatcher != null) {
			writeBatcher.flushAndWait();
		} else {
			logger.warn("writeBatcher is null - ignore this is you are running unit tests, otherwise this is a problem.");
		}
	}

	public String version() {
		return MarkLogicSinkConnector.MARKLOGIC_SINK_CONNECTOR_VERSION;
	}
}
