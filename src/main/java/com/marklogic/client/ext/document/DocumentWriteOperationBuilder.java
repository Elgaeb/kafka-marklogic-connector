package com.marklogic.client.ext.document;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public class DocumentWriteOperationBuilder {

	private DocumentWriteOperation.OperationType operationType = DocumentWriteOperation.OperationType.DOCUMENT_WRITE;
	private String uriPrefix;
	private String uriSuffix;
	private List<String> collections;
	private String permissions;

	private ContentIdExtractor contentIdExtractor = new DefaultContentIdExtractor();

	public DocumentWriteOperation build(SinkRecord sinkRecord, AbstractWriteHandle content, DocumentMetadataHandle metadata ) {
		if (content == null) {
			throw new NullPointerException("'content' must not be null");
		}

		if (collections != null) {
			metadata.getCollections().addAll(collections);
		}

		if (hasText(permissions)) {
			new DefaultDocumentPermissionsParser().parsePermissions(permissions.trim(), metadata.getPermissions());
		}

		String uri = buildUri(sinkRecord, content);
		return build(operationType, uri, metadata, content);
	}

	protected String buildUri(SinkRecord sinkRecord, AbstractWriteHandle content) {
		String contentId = contentIdExtractor.extractId(sinkRecord);
		String uri = contentId;
		if (hasText(uriPrefix)) {
			uri = uriPrefix + uri;
		}
		if (hasText(uriSuffix)) {
			uri += uriSuffix;
		}
		return uri;
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

	public DocumentWriteOperationBuilder withUriPrefix(String uriPrefix) {
		this.uriPrefix = uriPrefix;
		return this;
	}

	public DocumentWriteOperationBuilder withUriSuffix(String uriSuffix) {
		this.uriSuffix = uriSuffix;
		return this;
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

	public DocumentWriteOperationBuilder withContentIdExtractor(ContentIdExtractor contentIdExtractor) {
		this.contentIdExtractor = contentIdExtractor;
		return this;
	}
}
