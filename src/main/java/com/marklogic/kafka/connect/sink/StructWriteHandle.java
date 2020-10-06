package com.marklogic.kafka.connect.sink;

import com.marklogic.client.io.BaseHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.OutputStreamSender;
import com.marklogic.client.io.marker.JSONWriteHandle;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.Converter;

import java.io.IOException;
import java.io.OutputStream;

public class StructWriteHandle
        extends BaseHandle<byte[], OutputStreamSender>
        implements JSONWriteHandle,
        OutputStreamSender
{
    private Struct content;
    private Schema schema;

    protected Converter converter;

    public StructWriteHandle(Converter converter) {
        super();
        this.setResendable(true);

        this.converter = converter;
    }

    public void set(Schema schema, Struct content) {
        this.schema = schema;
        this.content = content;
    }

    public Struct get() {
        return this.content;
    }

    public StructWriteHandle withFormat(Format format) {
        setFormat(format);
        return this;
    }

    public StructWriteHandle withMimetype(String mimetype) {
        setMimetype(mimetype);
        return this;
    }

    public StructWriteHandle with(Schema schema, Struct content) {
        this.set(schema, content);
        return this;
    }

    @Override
    protected OutputStreamSender sendContent() {
        if (content == null) {
            throw new IllegalStateException("No Struct to write");
        }
        return this;
    }

    @Override
    public void write(OutputStream out) throws IOException {
        byte[] bytes = converter.fromConnectData(null, this.schema, this.content);
        out.write(bytes);
    }
}
