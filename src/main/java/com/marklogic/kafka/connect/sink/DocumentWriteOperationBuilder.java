package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.kafka.connect.sink.metadata.SourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.uri.URIFormatter;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

public class DocumentWriteOperationBuilder {

	private DocumentWriteOperation.OperationType operationType = DocumentWriteOperation.OperationType.DOCUMENT_WRITE;
	private List<String> collections;
	private String permissions;
	private Map<String, Object> kafkaConfig;
	private URIFormatter uriFormatter;

	private SourceMetadataExtractor sourceMetadataExtractor;

	public DocumentWriteOperationBuilder(Map<String, Object> kafkaConfig) {
		this.kafkaConfig = kafkaConfig;
	}

	public DocumentWriteOperation build(SinkRecord sinkRecord, AbstractWriteHandle content, DocumentMetadataHandle metadata) {
		if (content == null) {
			throw new NullPointerException("'content' must not be null");
		}

		if (collections != null) {
			metadata.getCollections().addAll(collections);
		}

		if (hasText(permissions)) {
			new DefaultDocumentPermissionsParser().parsePermissions(permissions.trim(), metadata.getPermissions());
		}

		this.sourceMetadataExtractor = SourceMetadataExtractor.newInstance(kafkaConfig);
		this.uriFormatter = new URIFormatter(kafkaConfig);

		String uri = buildUri(sinkRecord, content);
		return build(operationType, uri, metadata, content);
	}

	protected String buildUri(SinkRecord sinkRecord, AbstractWriteHandle content) {
		Map<String, Object> sourceMetadata = this.sourceMetadataExtractor.extract(sinkRecord);
		return this.uriFormatter.uri(sourceMetadata);
	}

	/**
	 * Exists to give a subclass a chance to further customize the contents of a DocumentWriteOperation before
	 * it's created.
	 *
	 * @param operationType
	 * @param uri
	 * @param metadata
	 * @param content
	 * @return
	 */
	protected DocumentWriteOperation build(DocumentWriteOperation.OperationType operationType, String uri, DocumentMetadataHandle metadata, AbstractWriteHandle content) {
		return new DocumentWriteOperationImpl(operationType, uri, metadata, content);
	}

	private boolean hasText(String val) {
		return val != null && val.trim().length() > 0;
	}

	public DocumentWriteOperationBuilder withCollections(List<String> collections) {
		this.collections = collections;
		return this;
	}

	public DocumentWriteOperationBuilder withPermissions(String permissions) {
		this.permissions = permissions;
		return this;
	}

	public DocumentWriteOperationBuilder withOperationType(DocumentWriteOperation.OperationType operationType) {
		this.operationType = operationType;
		return this;
	}
}
