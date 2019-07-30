package edu.si.trellis.query.rdf;

import static org.trellisldp.api.TrellisUtils.toDataset;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.si.trellis.ResyncResultSet;
import edu.si.trellis.query.AsyncResultSetSpliterator;
import edu.si.trellis.query.CassandraQuery;

import java.util.Spliterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.Quad;
import org.trellisldp.api.TrellisUtils;

/**
 * A query for use by individual resources to retrieve their contents.
 */
abstract class ResourceQuery extends CassandraQuery {

    static final String MUTABLE_TABLENAME = "mutabledata";

    static final String MEMENTO_MUTABLE_TABLENAME = "mementodata";

    static final String IMMUTABLE_TABLENAME = "immutabledata";

    static final String BASIC_CONTAINMENT_TABLENAME = "basiccontainment";

    ResourceQuery(CqlSession session, String queryString, ConsistencyLevel consistency) {
        super(session, queryString, consistency);
    }

    protected CompletionStage<Dataset> quads(final BoundStatement boundStatement) {
        CompletionStage<Stream<Dataset>> datasets = executeRead(boundStatement)
                        .thenApply(AsyncResultSetSpliterator::new)
                        .thenApply(rows -> StreamSupport.stream(rows, false).map(r -> r.get("quads", Dataset.class)));
        return datasets.thenApply(s -> s.flatMap(Dataset::stream).collect(toDataset()));
    }

}
