package com.marklogic.kafka.connect.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

interface FunctionalRecommender extends ConfigDef.Recommender {
	public default boolean visible(String name, Map<String, Object> parsedConfig) {
		return true;
	}
}

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
	public static final String CONNECTION_SIMPLE_SSL = "ml.connection.simpleSsl";
	public static final String CONNECTION_CERT_FILE = "ml.connection.certFile";
	public static final String CONNECTION_CERT_PASSWORD = "ml.connection.certPassword";
	public static final String CONNECTION_TRUSTSTORE_FILE = "ml.connection.truststoreFile";
	public static final String CONNECTION_TRUSTSTORE_PASSWORD = "ml.connection.truststorePassword";
	public static final String CONNECTION_TRUSTSTORE_TYPE = "ml.connection.truststoreType";
	public static final String CONNECTION_EXTERNAL_NAME = "ml.connection.externalName";

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
	public static final String DOCUMENT_COLLECTIONS = "ml.document.collections";
	public static final String DOCUMENT_PERMISSIONS = "ml.document.permissions";
	public static final String DOCUMENT_FORMAT = "ml.document.format";
	public static final String DOCUMENT_MIMETYPE = "ml.document.mimeType";
	public static final String DOCUMENT_URI_PREFIX = "ml.document.uriPrefix";
	public static final String DOCUMENT_URI_SUFFIX = "ml.document.uriSuffix";

	public static final String SSL = "ml.connection.enableCustomSsl";
	public static final String TLS_VERSION = "ml.connection.customSsl.tlsVersion";
	public static final String SSL_HOST_VERIFIER = "ml.connection.customSsl.hostNameVerifier";
	public static final String SSL_MUTUAL_AUTH = "ml.connection.customSsl.mutualAuth";

	private static final FunctionalRecommender KEYSTORE_TYPE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("pkcs12", "jks");
	private static final FunctionalRecommender HOSTNAME_VERIFIER_TYPE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("ANY", "COMMON", "STRICT");
	private static final FunctionalRecommender DOCUMENT_FORMAT_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("json", "xml", "text", "binary", "unknown");
	private static final FunctionalRecommender CONNECTION_TYPE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("DIRECT", "GATEWAY");
	private static final FunctionalRecommender SECURITY_CONTEXT_TYPE_RECOMMENDER = (name, parsedConfig) -> Arrays.asList("digest", "basic", "kerberos", "certificate", "none");

	public static ConfigDef CONFIG_DEF = new ConfigDef()
		.define(CONNECTION_HOST, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "MarkLogic server hostname", "Connection", 1, ConfigDef.Width.NONE, "MarkLogic Host")
		.define(CONNECTION_PORT, Type.INT, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "The REST app server port to connect to", "Connection", 2, ConfigDef.Width.NONE, "MarkLogic Port")
		.define(CONNECTION_DATABASE, Type.STRING, null, Importance.LOW, "Database to connect, if different from the one associated with the port", "Connection", 3, ConfigDef.Width.NONE, "MarkLogic Database")
		.define(CONNECTION_TYPE, Type.STRING, "DIRECT", Importance.LOW, "Connection type; DIRECT or GATEWAY", "Connection", 4, ConfigDef.Width.NONE, "Connection Type", Collections.emptyList(), CONNECTION_TYPE_RECOMMENDER)

		.define(SSL, Type.BOOLEAN, false, Importance.LOW, "Whether SSL connection to the App server - true or false.", "SSL", 1, ConfigDef.Width.NONE, "SSL")
		.define(CONNECTION_SIMPLE_SSL, Type.BOOLEAN, false, Importance.LOW, "Set to true to use a trust-everything SSL connection", "SSL", 2, ConfigDef.Width.NONE, "Use Simple SSL")
		.define(CONNECTION_TRUSTSTORE_FILE, Type.STRING, null, Importance.LOW, "Path to a truststore file", "SSL", 3, ConfigDef.Width.NONE, "Truststore")
		.define(CONNECTION_TRUSTSTORE_PASSWORD, Type.STRING, null, Importance.LOW, "Password for the truststore file", "SSL", 4, ConfigDef.Width.NONE, "Truststore Password")
		.define(CONNECTION_TRUSTSTORE_TYPE, Type.STRING, "pkcs12", Importance.LOW, "Type of the truststore: pkcs12 or jks", "SSL", 5, ConfigDef.Width.NONE, "Truststore Type", Collections.emptyList(), KEYSTORE_TYPE_RECOMMENDER)
		.define(TLS_VERSION, Type.STRING, null, Importance.LOW, "Version of TLS to connect to MarkLogic SSL enabled App server. Ex. TLSv1.2", "SSL", 6, ConfigDef.Width.NONE, "TLS Version")
		.define(SSL_HOST_VERIFIER, Type.STRING, "COMMON", Importance.LOW, "The strictness of Host Verifier - ANY, COMMON, STRICT", "SSL", 7, ConfigDef.Width.NONE, "Hostname Verifier", Collections.emptyList(), HOSTNAME_VERIFIER_TYPE_RECOMMENDER)
		.define(SSL_MUTUAL_AUTH, Type.BOOLEAN, false, Importance.LOW, "Mutual Authentication for Basic or Digest : true or false", "SSL", 8, ConfigDef.Width.NONE, "Mutual Authentication")

		.define(CONNECTION_SECURITY_CONTEXT_TYPE, Type.STRING, "digest", Importance.HIGH, "Type of MarkLogic security context to create - either digest, basic, kerberos, certificate, or none", "Authentication", 1, ConfigDef.Width.NONE, "Authentication Type", Collections.emptyList(), SECURITY_CONTEXT_TYPE_RECOMMENDER)
		.define(CONNECTION_USERNAME, Type.STRING, null, Importance.HIGH, "Name of MarkLogic user to authenticate as", "Authentication", 2, ConfigDef.Width.NONE, "MarkLogic Username")
		.define(CONNECTION_PASSWORD, Type.STRING, null, Importance.HIGH, "Password for the MarkLogic user", "Authentication", 3, ConfigDef.Width.NONE, "MarkLogic Password")
		.define(CONNECTION_EXTERNAL_NAME, Type.STRING, null, Importance.LOW, "External name for Kerberos authentication", "Authentication", 4, ConfigDef.Width.NONE, "Kerberos External Name")
		.define(CONNECTION_CERT_FILE, Type.STRING, null, Importance.LOW, "Path to a certificate file", "Authentication", 5, ConfigDef.Width.NONE, "Certificate File")
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

		.define(DOCUMENT_COLLECTIONS_ADD_TOPIC, Type.BOOLEAN, false, Importance.LOW, "Indicates if the topic name should be added to the set of collections for a document", "Documents", 1, ConfigDef.Width.NONE, "Add Topic as Collection")
		.define(DOCUMENT_COLLECTIONS, Type.STRING, null, Importance.MEDIUM, "String-delimited collections to add each document to", "Documents", 2, ConfigDef.Width.NONE, "Additional Collections")
		.define(DOCUMENT_FORMAT, Type.STRING, "json", Importance.LOW, "Defines format of each document; can be one of json, xml, text, binary, or unknown", "Documents", 3, ConfigDef.Width.NONE, "Output Format", Collections.emptyList(), DOCUMENT_FORMAT_RECOMMENDER)
		.define(DOCUMENT_MIMETYPE, Type.STRING, null, Importance.LOW, "Defines the mime type of each document; optional, and typically the format is set instead of the mime type", "Documents", 4, ConfigDef.Width.NONE, "MIME Type")
		.define(DOCUMENT_PERMISSIONS, Type.STRING, "rest-reader,read,rest-writer,update", Importance.MEDIUM, "String-delimited permissions to add to each document; role1,capability1,role2,capability2,etc", "Documents", 5, ConfigDef.Width.NONE, "Document Permissions")
		.define(DOCUMENT_URI_PREFIX, Type.STRING, null, Importance.MEDIUM, "Prefix to prepend to each generated URI", "Documents", 6, ConfigDef.Width.NONE, "URI Prefix")
		.define(DOCUMENT_URI_SUFFIX, Type.STRING, null, Importance.MEDIUM, "Suffix to append to each generated URI", "Documents", 7, ConfigDef.Width.NONE, "URI Suffix")

		;

	public MarkLogicSinkConfig(final Map<?, ?> originals) {
		super(CONFIG_DEF, originals, false);
	}

}
