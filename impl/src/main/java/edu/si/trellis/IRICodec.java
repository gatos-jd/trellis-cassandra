package edu.si.trellis;

import static com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder.newBuilder;
import static com.datastax.oss.driver.shaded.guava.common.cache.CacheLoader.from;
import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.cache.LoadingCache;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.UncheckedExecutionException;
import com.datastax.oss.protocol.internal.util.Bytes;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.trellisldp.api.TrellisUtils;

/**
 * (De)serializes Commons RDF {@link IRI}s (out of)into Cassandra fields.
 * 
 * @author ajs6f
 *
 */
class IRICodec implements TypeCodec<IRI> {

    private static final GenericType<IRI> TYPE_OF_IRI = GenericType.of(IRI.class);

    /**
     * Singleton instance.
     */
    static final IRICodec iriCodec = new IRICodec();

    protected static final int CACHE_CONCURRENCY_LEVEL = 16;

    protected static final long CACHE_MAXIMUM_SIZE = 10 ^ 6;

    protected static final RDF rdf = TrellisUtils.getInstance();

    private final LoadingCache<String, IRI> cache = newBuilder().concurrencyLevel(CACHE_CONCURRENCY_LEVEL)
                    .maximumSize(CACHE_MAXIMUM_SIZE).build(from(this::deserialize));

    private IRI deserialize(String v) {
        return rdf.createIRI(v);
    }

    @Override
    public String format(IRI v) {
        return v != null ? v.getIRIString() : null;
    }

    @Override
    public ByteBuffer encode(IRI iri, ProtocolVersion protocolVersion) {
        return iri != null ? wrap(format(iri).getBytes(UTF_8)) : null;
    }

    @Override
    public IRI decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null ? null : parse(new String(Bytes.getArray(bytes), UTF_8));
    }

    @Override
    public IRI parse(String v) {
        if (v == null || v.isEmpty()) return null;
        try {
            return cache.get(v);
        } catch (ExecutionException|UncheckedExecutionException e) {
            throw new IllegalArgumentException("Bad URI! " + v);
        }
    }

    @Override
    public GenericType<IRI> getJavaType() {
        return TYPE_OF_IRI;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.TEXT;
    }
}
