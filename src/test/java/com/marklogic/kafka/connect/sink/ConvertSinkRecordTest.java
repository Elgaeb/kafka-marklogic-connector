package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.document.DefaultContentIdExtractor;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import org.apache.kafka.connect.sink.SinkRecord;
 
import org.junit.jupiter.api.Test;


import java.util.*;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a DocumentWriteOperation is created correctly based on a SinkRecord.
 */
public class ConvertSinkRecordTest {

	DefaultSinkRecordConverter converter;
	MarkLogicSinkTask markLogicSinkTask = new MarkLogicSinkTask();

	@Test
	public void allPropertiesSet() throws IOException {
		Map<String, Object> config = new HashMap<>();
		config.put("ml.document.collections", Arrays.asList("one","two"));
		config.put("ml.document.format", "json");
		config.put("ml.document.mimeType", "application/json");
		config.put("ml.document.permissions", "manage-user,read,manage-admin,update");
		config.put("ml.document.uriPrefix", "/example/");
		config.put("ml.document.uriSuffix", ".json");
		config.put(MarkLogicSinkConfig.DOCUMENT_CONTENT_ID_EXTRACTOR, DefaultContentIdExtractor.class);
		converter = new DefaultSinkRecordConverter(config);

		DocumentWriteOperation op = converter.convert(newSinkRecord("test"));

		assertTrue(op.getUri().startsWith("/example/"));
		assertTrue(op.getUri().endsWith(".json"));
		

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		Iterator<String> collections = metadata.getCollections().iterator();
		assertEquals("one", collections.next());
		assertEquals("two", collections.next());

		DocumentMetadataHandle.DocumentPermissions perms = metadata.getPermissions();
		assertEquals(DocumentMetadataHandle.Capability.READ, perms.get("manage-user").iterator().next());
		assertEquals(DocumentMetadataHandle.Capability.UPDATE, perms.get("manage-admin").iterator().next());

		StringHandle content = (StringHandle)op.getContent();
		assertEquals("test", content.get());
		assertEquals(Format.JSON, content.getFormat());
		assertEquals("application/json", content.getMimetype());
	}

	@Test
	public void noPropertiesSet() throws IOException {
		Map<String, Object> config = new HashMap<>();
		config.put(MarkLogicSinkConfig.DOCUMENT_CONTENT_ID_EXTRACTOR, DefaultContentIdExtractor.class);
		converter = new DefaultSinkRecordConverter(config);

		converter.getDocumentWriteOperationBuilder().withContentIdExtractor((sinkRecord) -> "12345");

		DocumentWriteOperation op = converter.convert(newSinkRecord("doesn't matter"));

		assertEquals("12345", op.getUri());

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		assertTrue(metadata.getCollections().isEmpty());
		assertTrue(metadata.getPermissions().isEmpty());
	}

	@Test
	public void binaryContent() throws IOException{
		Map<String, Object> config = new HashMap<>();
		config.put(MarkLogicSinkConfig.DOCUMENT_CONTENT_ID_EXTRACTOR, DefaultContentIdExtractor.class);
		converter = new DefaultSinkRecordConverter(config);

		DocumentWriteOperation op = converter.convert(newSinkRecord("hello world".getBytes()));

		BytesHandle content = (BytesHandle)op.getContent();
		assertEquals("hello world".getBytes().length, content.get().length);
	}

	@Test
	public void emptyContent() {
		final Collection<SinkRecord> records = new ArrayList<>();
		records.add(newSinkRecord(null));
		markLogicSinkTask.put(records);
	}

	@Test
	public void nullContent() {
		final Collection<SinkRecord> records = new ArrayList<>();
		records.add(null);
		markLogicSinkTask.put(records);
	}

	private SinkRecord newSinkRecord(Object value) {
		return new SinkRecord("test-topic", 1, null, null, null, value, 0);
	}
}
