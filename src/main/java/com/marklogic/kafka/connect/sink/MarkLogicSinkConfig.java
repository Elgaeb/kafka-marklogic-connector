package com.marklogic.kafka.connect.sink;

import com.marklogic.kafka.connect.sink.metadata.ConfluentOracleSourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.metadata.DebeziumOracleSourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.metadata.UUIDSourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.recordconverter.ConfluentOracleJSONSinkRecordConverter;
import com.marklogic.kafka.connect.sink.recordconverter.ConnectSinkRecordConverter;
import com.marklogic.kafka.connect.sink.recordconverter.DHFEnvelopeSinkRecordConverter;
import com.marklogic.kafka.connect.sink.recordconverter.DebeziumOracleSinkRecordConverter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Defines configuration properties for the MarkLogic sink connector.
 */
public class MarkLogicSinkConfig extends AbstractConfig {

	public static final String CONNECTION_HOST = "ml.connection.host";
	public static final String CONNECTION_PORT = "ml.connection.port";
	public static final String CONNECTION_DATABASE = "ml.connection.database";
	public static final String CONNECTION_SECURITY_CONTEXT_TYPE = "ml.connection.securityContextType";
	public static final String CONNECTION_USERNAME = "ml.connection.username";
	public static final String CONNECTION_PASSWORD = "ml.connection.password";
	public static final String CONNECTION_TYPE = "ml.connection.type";
	public static final String CONNECTION_CERT_FILE = "ml.connection.certFile";
	public static final String CONNECTION_CERT_PASSWORD = "ml.connection.cert.password";
	public static final String CONNECTION_TRUSTSTORE_URL = "ml.connection.truststore.url";
	public static final String CONNECTION_TRUSTSTORE_PASSWORD = "ml.connection.truststore.password";
	public static final String CONNECTION_TRUSTSTORE_FORMAT = "ml.connection.truststore.format";
	public static final String CONNECTION_EXTERNAL_NAME = "ml.connection.externalName";

	public static final String DOCUMENT_SOURCE_METADATA_EXTRACTOR = "ml.document.sourceMetadataExtractor";

	public static final String DATAHUB_FLOW_NAME = "ml.datahub.flow.name";
	public static final String DATAHUB_FLOW_STEPS = "ml.datahub.flow.steps";
	public static final String DATAHUB_FLOW_LOG_RESPONSE = "ml.datahub.flow.logResponse";

	public static final String DMSDK_BATCH_SIZE = "ml.dmsdk.batchSize";
	public static final String DMSDK_THREAD_COUNT = "ml.dmsdk.threadCount";
	public static final String DMSDK_TRANSFORM = "ml.dmsdk.transform";
	public static final String DMSDK_TRANSFORM_PARAMS = "ml.dmsdk.transformParams";
	public static final String DMSDK_TRANSFORM_PARAMS_DELIMITER = "ml.dmsdk.transformParamsDelimiter";

	public static final String DMSDK_FLUSH_ON_PUT = "ml.document.flushOnPut";
	public static final String DOCUMENT_COLLECTIONS_ADD_TOPIC = "ml.document.addTopicToCollections";
	public static final String DOCUMENT_TOPIC_COLLECTION_CASE = "ml.document.topicCollectionFormat";
	public static final String DOCUMENT_COLLECTIONS = "ml.document.collections";
	public static final String DOCUMENT_PERMISSIONS = "ml.document.permissions";
	public static final String DOCUMENT_FORMAT = "ml.document.format";
	public static final String DOCUMENT_MIMETYPE = "ml.document.mimeType";
	public static final String DOCUMENT_CONVERTER = "ml.document.sinkRecordConverter";

	public static final String SSL_CONNECTION_TYPE = "ml.connection.sslConnectionType";
	public static final String TLS_VERSION = "ml.connection.customSsl.tlsVersion";
	public static final String SSL_HOST_VERIFIER = "ml.connection.customSsl.hostNameVerifier";
	public static final String SSL_MUTUAL_AUTH = "ml.connection.customSsl.mutualAuth";

	public static final String CSRC_URI_FORMATSTRING = "ml.connectSinkRecordConverter.uriFormat";
	public static final String CSRC_URI_CASE = "ml.connectSinkRecordConverter.uriCase";
	public static final String CSRC_COLLECTION_FORMATSTRING = "ml.connectSinkRecordConverter.collectionFormat";
	public static final String CSRC_COLLECTION_CASE = "ml.connectSinkRecordConverter.collectionCase";

	private static final AlwaysVisibleRecommender KEYSTORE_TYPE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("pkcs12", "jks");
	private static final AlwaysVisibleRecommender HOSTNAME_VERIFIER_TYPE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("ANY", "COMMON", "STRICT");
	private static final AlwaysVisibleRecommender DOCUMENT_FORMAT_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("json", "xml", "text", "binary", "unknown");
	private static final AlwaysVisibleRecommender CONNECTION_TYPE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("DIRECT", "GATEWAY");
	private static final AlwaysVisibleRecommender SECURITY_CONTEXT_TYPE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("digest", "basic", "kerberos", "certificate", "none");
	private static final AlwaysVisibleRecommender SSL_CONNECTION_TYPE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("none", "simple", "default", "custom");
	private static final AlwaysVisibleRecommender SOURCE_METADATA_EXTRACTOR_RECOMMENDER = (name, parsedConfig) -> Arrays.asList(DebeziumOracleSourceMetadataExtractor.class, UUIDSourceMetadataExtractor.class, ConfluentOracleSourceMetadataExtractor.class);
	private static final AlwaysVisibleRecommender SINK_RECORD_CONVERTER_RECOMMENDER = (name, parsedConfig) -> Arrays.asList(DHFEnvelopeSinkRecordConverter.class, ConnectSinkRecordConverter.class, DebeziumOracleSinkRecordConverter.class, DHFEnvelopeSinkRecordConverter.class, ConfluentOracleJSONSinkRecordConverter.class);
	private static final AlwaysVisibleRecommender STRING_CASE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("as-is", "lower", "upper", "camel");

	public static ConfigDef CONFIG_DEF = new ConfigDef()
		.define(CONNECTION_HOST, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "MarkLogic server hostname", "Connection", 1, ConfigDef.Width.NONE, "MarkLogic Host")
		.define(CONNECTION_PORT, Type.INT, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "The REST app server port to connect to", "Connection", 2, ConfigDef.Width.NONE, "MarkLogic Port")
		.define(CONNECTION_DATABASE, Type.STRING, null, Importance.LOW, "Database to connect, if different from the one associated with the port", "Connection", 3, ConfigDef.Width.NONE, "MarkLogic Database")
		.define(CONNECTION_TYPE, Type.STRING, "DIRECT", Importance.LOW, "Connection type; DIRECT or GATEWAY", "Connection", 4, ConfigDef.Width.NONE, "Connection Type", Collections.emptyList(), CONNECTION_TYPE_RECOMMENDER)

		.define(SSL_CONNECTION_TYPE, Type.STRING, "none", Importance.HIGH, "Whether SSL connection to MarkLogic. none: do not use SSL, simple: use a trust-everything SSL connection, default: use the system default truststore, custom: use a custom truststore", "SSL", 1, ConfigDef.Width.NONE, "SSL", Collections.emptyList(), SSL_CONNECTION_TYPE_RECOMMENDER)
		.define(CONNECTION_TRUSTSTORE_URL, Type.STRING, null, Importance.LOW, "Path to a truststore file", "SSL", 3, ConfigDef.Width.NONE, "Truststore")
		.define(CONNECTION_TRUSTSTORE_PASSWORD, Type.STRING, null, Importance.LOW, "Password for the truststore file", "SSL", 4, ConfigDef.Width.NONE, "Truststore Password")
		.define(CONNECTION_TRUSTSTORE_FORMAT, Type.STRING, "pkcs12", Importance.LOW, "Type of the truststore: pkcs12 or jks", "SSL", 5, ConfigDef.Width.NONE, "Truststore Type", Collections.emptyList(), KEYSTORE_TYPE_RECOMMENDER)
		.define(TLS_VERSION, Type.STRING, "TLSv1.2", Importance.LOW, "Version of TLS to connect to MarkLogic SSL enabled App server. Ex. TLSv1.2", "SSL", 6, ConfigDef.Width.NONE, "TLS Version")
		.define(SSL_HOST_VERIFIER, Type.STRING, "ANY", Importance.LOW, "The strictness of Host Verifier - ANY, COMMON, STRICT", "SSL", 7, ConfigDef.Width.NONE, "Hostname Verifier", Collections.emptyList(), HOSTNAME_VERIFIER_TYPE_RECOMMENDER)
		.define(SSL_MUTUAL_AUTH, Type.BOOLEAN, false, Importance.LOW, "Mutual Authentication for Basic or Digest : true or false", "SSL", 8, ConfigDef.Width.NONE, "Mutual Authentication")

		.define(CONNECTION_SECURITY_CONTEXT_TYPE, Type.STRING, "digest", Importance.HIGH, "Type of MarkLogic security context to create - either digest, basic, kerberos, certificate, or none", "Authentication", 1, ConfigDef.Width.NONE, "Authentication Type", Collections.emptyList(), SECURITY_CONTEXT_TYPE_RECOMMENDER)
		.define(CONNECTION_USERNAME, Type.STRING, null, Importance.HIGH, "Name of MarkLogic user to authenticate as", "Authentication", 2, ConfigDef.Width.NONE, "MarkLogic Username")
		.define(CONNECTION_PASSWORD, Type.STRING, null, Importance.HIGH, "Password for the MarkLogic user", "Authentication", 3, ConfigDef.Width.NONE, "MarkLogic Password")
		.define(CONNECTION_EXTERNAL_NAME, Type.STRING, null, Importance.LOW, "External name for Kerberos authentication", "Authentication", 4, ConfigDef.Width.NONE, "Kerberos External Name")
		.define(CONNECTION_CERT_FILE, Type.STRING, null, Importance.LOW, "Path to a certificate PKCS12 file", "Authentication", 5, ConfigDef.Width.NONE, "Certificate File")
		.define(CONNECTION_CERT_PASSWORD, Type.STRING, null, Importance.LOW, "Password for the certificate file", "Authentication", 6, ConfigDef.Width.NONE, "Certificate Password")

		.define(DATAHUB_FLOW_NAME, Type.STRING, null, Importance.MEDIUM, "Name of a Data Hub flow to run", "Data Hub Framework", 1, ConfigDef.Width.NONE, "Flow Name")
		.define(DATAHUB_FLOW_STEPS, Type.STRING, null, Importance.MEDIUM, "Comma-delimited names of steps to run", "Data Hub Framework", 2, ConfigDef.Width.NONE, "Flow Steps")
		.define(DATAHUB_FLOW_LOG_RESPONSE, Type.BOOLEAN, false, Importance.LOW, "If set to true, the response from running a flow on each ingested batch will be logged at the info level", "Data Hub Framework", 3, ConfigDef.Width.NONE, "Log Flow Response")

		.define(DMSDK_BATCH_SIZE, Type.INT, 100, Importance.HIGH, "Number of documents to write in each batch", "DMSDK", 1, ConfigDef.Width.NONE, "Batch Size")
		.define(DMSDK_THREAD_COUNT, Type.INT, 8, Importance.HIGH, "Number of threads for DMSDK to use", "DMSDK", 2, ConfigDef.Width.NONE, "Thread Count")
		.define(DMSDK_TRANSFORM, Type.STRING, null, Importance.MEDIUM, "Name of a REST transform to use when writing documents", "DMSDK", 3, ConfigDef.Width.NONE, "Transform name")
		.define(DMSDK_TRANSFORM_PARAMS, Type.STRING, null, Importance.MEDIUM, "Delimited set of transform names and values", "DMSDK", 4, ConfigDef.Width.NONE, "Transform Parameters")
		.define(DMSDK_TRANSFORM_PARAMS_DELIMITER, Type.STRING, ",", Importance.LOW, "Delimiter for transform parameter names and values; defaults to a comma", "DMSDK", 5, ConfigDef.Width.NONE, "Transform Parameter Delimiter")
		.define(DMSDK_FLUSH_ON_PUT, Type.BOOLEAN, true, Importance.LOW, "Whether to flush written documents to MarkLogic after each put.", "DMSDK", 6, ConfigDef.Width.NONE, "Flush Documents on Put")

		.define(DOCUMENT_COLLECTIONS, Type.LIST, null, Importance.MEDIUM, "String-delimited collections to add each document to", "Documents", 1, ConfigDef.Width.NONE, "Additional Collections")
		.define(DOCUMENT_PERMISSIONS, Type.STRING, "rest-reader,read,rest-writer,update", Importance.MEDIUM, "String-delimited permissions to add to each document; role1,capability1,role2,capability2,etc", "Documents", 2, ConfigDef.Width.NONE, "Document Permissions")
		.define(DOCUMENT_SOURCE_METADATA_EXTRACTOR, Type.CLASS, UUIDSourceMetadataExtractor.class, Importance.MEDIUM, "Class used to extract metadata from source message.", "Documents", 3, ConfigDef.Width.NONE, "Source Metadata Extractor", Collections.emptyList(), SOURCE_METADATA_EXTRACTOR_RECOMMENDER)
		.define(DOCUMENT_CONVERTER, Type.CLASS, DHFEnvelopeSinkRecordConverter.class, Importance.LOW, "Defines format of each document; can be one of json, xml, text, binary, or unknown", "Documents", 4, ConfigDef.Width.NONE, "Record Converter", Collections.emptyList(), SINK_RECORD_CONVERTER_RECOMMENDER)

		.define(DOCUMENT_COLLECTIONS_ADD_TOPIC, Type.BOOLEAN, false, Importance.LOW, "Indicates if the topic name should be added to the set of collections for a document", "DefaultSinkRecordConverter", 3, ConfigDef.Width.NONE, "Add Topic as Collection")
		.define(DOCUMENT_TOPIC_COLLECTION_CASE, Type.STRING, "as-is", Importance.LOW, "Case used for the topic collection: as-is, lower, upper, camel", "DefaultSinkRecordConverter", 4, ConfigDef.Width.NONE, "Collection Case", Collections.emptyList(), STRING_CASE_RECOMMENDER)
		.define(DOCUMENT_FORMAT, Type.STRING, "json", Importance.LOW, "Defines format of each document; can be one of json, xml, text, binary, or unknown", "DefaultSinkRecordConverter", 5, ConfigDef.Width.NONE, "Output Format", Collections.emptyList(), DOCUMENT_FORMAT_RECOMMENDER)
		.define(DOCUMENT_MIMETYPE, Type.STRING, null, Importance.LOW, "Defines the mime type of each document; optional, and typically the format is set instead of the mime type", "DefaultSinkRecordConverter", 6, ConfigDef.Width.NONE, "MIME Type")

		.define(CSRC_URI_FORMATSTRING, Type.STRING, "${id}", Importance.MEDIUM, "Format string used to generate URIs.", "ConnectSinkRecordConverter", 2, ConfigDef.Width.NONE, "URI Format")
		.define(CSRC_URI_CASE, Type.STRING, "as-is", Importance.MEDIUM, "Case used for the generated uri: as-is, lower, upper, camel", "ConnectSinkRecordConverter", 3, ConfigDef.Width.NONE, "URI Case", Collections.emptyList(), STRING_CASE_RECOMMENDER)
		.define(CSRC_COLLECTION_FORMATSTRING, Type.STRING, null, Importance.MEDIUM, "Format string used to generate an additional collection for documents.", "ConnectSinkRecordConverter", 4, ConfigDef.Width.NONE, "Collection Format")
		.define(CSRC_COLLECTION_CASE, Type.STRING, "as-is", Importance.MEDIUM, "Case used for the generated collection: as-is, lower, upper, camel", "ConnectSinkRecordConverter", 5, ConfigDef.Width.NONE, "Collection Case", Collections.emptyList(), STRING_CASE_RECOMMENDER)
		;

	public MarkLogicSinkConfig(final Map<?, ?> originals) {
		super(CONFIG_DEF, originals, false);
	}

	public static Map<String, Object> hydrate(Map<String, ? extends Object> original) {
		Map<String, Object> hydrated = new HashMap<>(original);

		CONFIG_DEF.configKeys().forEach((name, configKey) -> {
			Object inputValue = original.get(configKey.name);
			Object hydratedValue = null;
			if(inputValue != null) {
				hydratedValue = ConfigDef.parseType(configKey.name, inputValue, configKey.type);
			} else {
				if(!ConfigDef.NO_DEFAULT_VALUE.equals(configKey.defaultValue)) {
					hydratedValue = configKey.defaultValue;
				}
			}

			if(hydratedValue != null) {
				hydrated.put(configKey.name, hydratedValue);
			}
		});

		return hydrated;
	}
}
