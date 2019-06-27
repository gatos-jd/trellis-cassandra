package edu.si.trellis;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;

import java.io.InputStream;
import java.util.concurrent.CompletionStage;

/**
 * An {@link InputStream} backed by a Cassandra query to retrieve one binary chunk.
 * <p>
 * Not thread-safe!
 * </p>
 * 
 * @see InputStreamCodec
 */
public class LazyChunkInputStream extends LazyFilterInputStream {

    private final CompletionStage<AsyncResultSet> futureData;

    /**
     * @param session The Cassandra session to use
     * @param query the CQL query to use
     */
    public LazyChunkInputStream(CqlSession session, BoundStatement query) {
        this.futureData = session.executeAsync(query);
    }

    @Override
    protected void initialize() {
        Row row = requireNonNull(futureData.toCompletableFuture().join().one(), "Missing binary chunk!");
        wrap(row.get("chunk", InputStream.class));
    }
}
