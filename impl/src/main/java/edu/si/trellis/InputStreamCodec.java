package edu.si.trellis;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;

/**
 * Serializes {@link InputStream}s in Cassandra text fields.
 *
 */
class InputStreamCodec implements TypeCodec<InputStream> {

    private static final GenericType<InputStream> TYPE_OF_INPUTSTREAM = GenericType.of(InputStream.class);
    public static final InputStreamCodec inputStreamCodec = new InputStreamCodec();

    @Override
    public ByteBuffer encode(InputStream value, ProtocolVersion protocolVersion) {
        return value == null ? null : ByteBuffer.wrap(toBytes(value));
    }

    @Override
    public InputStream decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null ? null : new ByteBufferInputStream(bytes);
    }

    @Override
    public InputStream parse(String value) {
        return value == null ? null : new ByteArrayInputStream(value.getBytes(UTF_8));
    }

    private static byte[] toBytes(InputStream in) {
        try {
            return IOUtils.toByteArray(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String format(InputStream in) {
        return in == null ? null : new String(toBytes(in), UTF_8);
    }

    @Override
    public GenericType<InputStream> getJavaType() {
        return TYPE_OF_INPUTSTREAM;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.BLOB;
    }
}
