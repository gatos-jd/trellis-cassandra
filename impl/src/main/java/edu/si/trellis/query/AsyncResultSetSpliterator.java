package edu.si.trellis.query;

import static java.util.Spliterators.spliteratorUnknownSize;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * A simple {@link Spliterator} backed by a {@link AsyncResultSet}.
 *
 * Not thread-safe!
 */
public class AsyncResultSetSpliterator implements Spliterator<Row> {

    private final AsyncResultSet results;

    private Iterator<Row> currentResults;

    private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.NONNULL;

    public AsyncResultSetSpliterator(AsyncResultSet r) {
        this.results = r;
        this.currentResults = r.currentPage().iterator();
    }

    @Override
    public boolean tryAdvance(Consumer<? super Row> action) {
        if (currentResults.hasNext()) {
            action.accept(currentResults.next());
            return true;
        } else if (results.hasMorePages()) {
            nextPage();
            return true;
        }
        return false;
    }

    @Override
    public Spliterator<Row> trySplit() {
        if (results.hasMorePages()) {
            final Iterator<Row> splitResults = currentResults;
            nextPage();
            return spliteratorUnknownSize(splitResults, 0);
        }
        return null;
    }

    private void nextPage() {
        if (currentResults.hasNext()) return;
        results.fetchNextPage();
        currentResults = results.currentPage().iterator();
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return CHARACTERISTICS;
    }

}
