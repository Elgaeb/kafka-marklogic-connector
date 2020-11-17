package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.kafka.connect.sink.metadata.SourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.metadata.UUIDSourceMetadataExtractor;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.kafka.connect.sink.recordconverter.DHFEnvelopeSinkRecordConverter;
import com.marklogic.kafka.connect.sink.util.HashMapBuilder;
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

	DHFEnvelopeSinkRecordConverter converter;
	MarkLogicSinkTask markLogicSinkTask = new MarkLogicSinkTask();

	@Test
	public void allPropertiesSet() throws IOException {
		Map<String, Object> config = new HashMap<>();
		config.put("ml.document.collections", Arrays.asList("one","two"));
		config.put("ml.document.format", "json");
		config.put("ml.document.mimeType", "application/json");
		config.put("ml.document.permissions", "manage-user,read,manage-admin,update");
		config.put(MarkLogicSinkConfig.CSRC_URI_FORMATSTRING, "/example/${id}.json");
		config.put(MarkLogicSinkConfig.DOCUMENT_SOURCE_METADATA_EXTRACTOR, UUIDSourceMetadataExtractor.class);
		converter = new DHFEnvelopeSinkRecordConverter(config);

		UpdateOperation updateOperation = converter.convert(newSinkRecord("test"));
		assertEquals(1, updateOperation.getWrites().size());
		DocumentWriteOperation op = updateOperation.getWrites().get(0);

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

	public static class StaticIDSourceMetadataExtractor  implements SourceMetadataExtractor {
		@Override
		public Map<String, Object> extract(SinkRecord sinkRecord) {
			return new HashMapBuilder().with("id", "12345");
		}
	}

	@Test
	public void noPropertiesSet() throws IOException {
		Map<String, Object> config = new HashMap<>();
		config.put(MarkLogicSinkConfig.DOCUMENT_SOURCE_METADATA_EXTRACTOR, StaticIDSourceMetadataExtractor.class);
		converter = new DHFEnvelopeSinkRecordConverter(config);

//		converter.getDocumentWriteOperationBuilder().withContentIdExtractor((sinkRecord) -> "12345");

		UpdateOperation updateOperation = converter.convert(newSinkRecord("doesn't matter"));
		assertEquals(1, updateOperation.getWrites().size());
		DocumentWriteOperation op = updateOperation.getWrites().get(0);

		assertEquals("12345", op.getUri());

		DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
		assertTrue(metadata.getCollections().isEmpty());
		assertTrue(metadata.getPermissions().isEmpty());
	}

	@Test
	public void binaryContent() throws IOException{
		Map<String, Object> config = new HashMap<>();
		config.put(MarkLogicSinkConfig.DOCUMENT_SOURCE_METADATA_EXTRACTOR, UUIDSourceMetadataExtractor.class);
//		config.put(MarkLogicSinkConfig.DOCUMENT_CONTENT_ID_EXTRACTOR, DefaultContentIdExtractor.class);
		converter = new DHFEnvelopeSinkRecordConverter(config);

		UpdateOperation updateOperation = converter.convert(newSinkRecord("hello world".getBytes()));
		assertEquals(1, updateOperation.getWrites().size());
		DocumentWriteOperation op = updateOperation.getWrites().get(0);

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
