package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.si.trellis.MutableReadConsistency;

import java.time.Instant;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

/**
 * A query to retrieve (formerly) mutable data about a Memento from Cassandra.
 */
public class MementoMutableRetrieve extends ResourceQuery {

    @Inject
    public MementoMutableRetrieve(CqlSession session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT quads FROM " + MEMENTO_MUTABLE_TABLENAME
                        + " WHERE identifier = :identifier AND mementomodified <= :time " + "LIMIT 1 ALLOW FILTERING;",
                        consistency);
    }

    /**
     * @param id the {@link IRI} of the Memento, the RDF of which is to be retrieved
     * @param time the time for which this Memento must be valid
     * @return the RDF retrieved
     */
    public Stream<Quad> execute(IRI id, Instant time) {
        return quads(preparedStatement().bind()
                        .set("time", time, Instant.class)
                        .set("identifier", id, IRI.class));
    }
}
