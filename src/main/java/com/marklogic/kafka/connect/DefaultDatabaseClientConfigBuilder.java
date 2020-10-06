package com.marklogic.kafka.connect;

import javax.net.ssl.*;
import java.net.URL;
import java.security.*;
import java.util.Map;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import javax.net.ssl.SSLContext;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.ext.DatabaseClientConfig;
import com.marklogic.client.ext.SecurityContextType;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;

import com.marklogic.client.ext.modulesloader.ssl.SimpleX509TrustManager;

public class DefaultDatabaseClientConfigBuilder implements DatabaseClientConfigBuilder {

	@Override
	public DatabaseClientConfig buildDatabaseClientConfig(Map<String, Object> kafkaConfig) {
		DatabaseClientConfig clientConfig = new DatabaseClientConfig();
		clientConfig.setCertFile((String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_CERT_FILE));
		clientConfig.setCertPassword((String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_CERT_PASSWORD));

		String securityContextType = ((String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_SECURITY_CONTEXT_TYPE)).toUpperCase();
		clientConfig.setSecurityContextType(SecurityContextType.valueOf(securityContextType));

		configureSSLContext(clientConfig, kafkaConfig);

		String database = (String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_DATABASE);
		if (database != null && database.trim().length() > 0) {
			clientConfig.setDatabase(database);
		}
		String connType = (String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_TYPE);
		if (connType != null && connType.trim().length() > 0) {
			clientConfig.setConnectionType(DatabaseClient.ConnectionType.valueOf(connType.toUpperCase()));
		}
		clientConfig.setExternalName((String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_EXTERNAL_NAME));
		clientConfig.setHost((String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_HOST));
		clientConfig.setPassword((String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_PASSWORD));
		clientConfig.setPort((Integer) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_PORT));
		clientConfig.setUsername((String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_USERNAME));
		return clientConfig;
	}

	protected DatabaseClientConfig configureSSLContext(DatabaseClientConfig clientConfig, Map<String, Object> kafkaConfig) {
		try {
			X509TrustManager trustManager = null;
			switch (((String) kafkaConfig.getOrDefault(MarkLogicSinkConfig.SSL_CONNECTION_TYPE, "none")).toLowerCase()) {
				case "simple":
					trustManager = new SimpleX509TrustManager();
					break;
				case "default":
					trustManager = DefaultDatabaseClientConfigBuilder.getDefaultTrustManager();
					break;
				case "custom":
					String truststoreLocation = (String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_TRUSTSTORE_URL);
					String truststoreFormat = (String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_TRUSTSTORE_FORMAT);
					String truststorePassword = (String) kafkaConfig.get(MarkLogicSinkConfig.CONNECTION_TRUSTSTORE_PASSWORD);
					trustManager = DefaultDatabaseClientConfigBuilder.getTrustManager(truststoreLocation, truststoreFormat, truststorePassword);
					break;
				case "none":
				default:
					// go ahead and abort if we're not using ssl
					return clientConfig;
			}

			switch(((String) kafkaConfig.getOrDefault(MarkLogicSinkConfig.SSL_HOST_VERIFIER, "common")).toUpperCase()) {
				case "STRICT":
					clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.STRICT);
					break;
				case "COMMON":
					clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.COMMON);
					break;
				case "ANY":
				default:
					clientConfig.setSslHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
					break;
			}

			KeyManager[] keyManagers = null;
			Boolean sslMutualAuth = (Boolean) kafkaConfig.get(MarkLogicSinkConfig.SSL_MUTUAL_AUTH);
			if(sslMutualAuth != null && sslMutualAuth) {
				KeyStore clientKeyStore = KeyStore.getInstance("PKCS12");
				try (InputStream keystoreInputStream = new FileInputStream(clientConfig.getCertFile())) {
					clientKeyStore.load(keystoreInputStream, clientConfig.getCertPassword().toCharArray());
				}
				KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
				keyManagerFactory.init(clientKeyStore, clientConfig.getCertPassword().toCharArray());
				keyManagers = keyManagerFactory.getKeyManagers();
			}

			if(trustManager != null) {
				TrustManager[] trustManagers = { trustManager };

				String tlsVersion = (String) kafkaConfig.get(MarkLogicSinkConfig.TLS_VERSION);
				SSLContext sslContext = SSLContext.getInstance((tlsVersion != null && tlsVersion.trim().length() > 0 ) ? tlsVersion : "TLSv1.2");
				sslContext.init(keyManagers, trustManagers, new SecureRandom());

				clientConfig.setTrustManager(trustManager);
				clientConfig.setSslContext(sslContext);
			}
		} catch(IOException | NoSuchAlgorithmException | KeyStoreException | CertificateException | KeyManagementException | UnrecoverableKeyException ex) {
			throw new RuntimeException("Error configuring SSL", ex);
		}

		return clientConfig;
	}

	public static X509TrustManager getDefaultTrustManager() throws NoSuchAlgorithmException, KeyStoreException {
		TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init((KeyStore) null);
		for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
			if(trustManager instanceof X509TrustManager) {
				return (X509TrustManager) trustManager;
			}
		}
		return null;
	}

	public static X509TrustManager getTrustManager(String truststoreLocation, String truststoreFormat, String truststorePassword) throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException {
		KeyStore truststore = KeyStore.getInstance(truststoreFormat);

		try(InputStream truststoreInput = new URL(truststoreLocation).openStream()) {
			truststore.load(truststoreInput, truststorePassword.toCharArray());
		}

		TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustManagerFactory.init(truststore);

		for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
			if(trustManager instanceof X509TrustManager) {
				return (X509TrustManager) trustManager;
			}
		}

		return null;
	}

}
