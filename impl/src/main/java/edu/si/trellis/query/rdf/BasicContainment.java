package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;

import edu.si.trellis.MutableReadConsistency;

import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query to retrieve basic containment information from a materialized view or index table.
 */
public class BasicContainment extends ResourceQuery {

    @Inject
    public BasicContainment(CqlSession session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT identifier AS contained FROM " + BASIC_CONTAINMENT_TABLENAME
                        + " WHERE container = :container ;", consistency);
    }

    /**
     * @param id the {@link IRI} of the container
     * @return a {@link ResultSet} of the resources contained in {@code id}
     */
    public CompletionStage<AsyncResultSet> execute(IRI id) {
        return executeRead(preparedStatement().bind().set("container", id, IRI.class));
    }
}
