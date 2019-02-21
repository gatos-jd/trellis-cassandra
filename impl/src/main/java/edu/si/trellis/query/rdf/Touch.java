package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfWriteConsistency;
import edu.si.trellis.query.CassandraQuery;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

/**
 * A query that adjusts the modified time of a resource.
 */
public class Touch extends CassandraQuery {

    @Inject
    public Touch(Session session, @RdfWriteConsistency ConsistencyLevel consistency) {
        super(session, "UPDATE " + MUTABLE_TABLENAME + " SET modified=? WHERE created=? AND identifier=?", consistency);
    }

    /**
     * @param modified the new modification time to record
     * @param created the time-based (version 1) UUID that, together with the IRI in {@code id}, uniquely identifies a
     *            version of a resource
     * @param id the {@link IRI} of the resource to modify
     * @return whether and when the modification succeeds
     */
    public CompletableFuture<Void> execute(Instant modified, UUID created, IRI id) {
        return executeWrite(preparedStatement().bind(modified, created, id));
    }
}