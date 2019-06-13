package edu.si.trellis;

import static java.util.Collections.singletonList;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

/**
 * A simple {@link ResultSet} that is backed by an {@link AsyncResultSet}.
 * 
 * Not thread safe.
 *
 */
public class ResyncResultSet implements ResultSet {

    private final AsyncResultSet results;

    public ResyncResultSet(AsyncResultSet results) {
        this.results = results;
    }

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return results.getColumnDefinitions();
    }

    @Override
    public List<ExecutionInfo> getExecutionInfos() {
        return singletonList(results.getExecutionInfo());
    }

    @Override
    public boolean isFullyFetched() {
        return results.hasMorePages();
    }

    @Override
    public int getAvailableWithoutFetching() {
        return results.remaining();
    }

    @Override
    public Iterator<Row> iterator() {
        return new AsyncResultSetIterator(results);
    }

    @Override
    public boolean wasApplied() {
        return results.wasApplied();
    }

    /**
     * Uses a stacked method call per async page.
     *
     */
    private static class AsyncResultSetIterator implements Iterator<Row> {

        private AsyncResultSet results;

        private AsyncResultSetIterator(AsyncResultSet results) {
            this.results = results;
        }

        @Override
        public boolean hasNext() {
            return results.remaining() > 0 || results.hasMorePages();
        }

        @Override
        public Row next() {
            if (results.remaining() > 0) return results.one();
            if (results.hasMorePages()) try {
                results = results.fetchNextPage().toCompletableFuture().get();
                return next();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
            throw new NoSuchElementException();
        }

    }
}
