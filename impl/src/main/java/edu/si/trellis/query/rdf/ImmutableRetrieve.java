package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.si.trellis.MutableReadConsistency;

import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

/**
 * A query to retrieve immutable data about a resource from Cassandra.
 */
public class ImmutableRetrieve extends ResourceQuery {

    @Inject
    public ImmutableRetrieve(CqlSession session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT quads FROM " + IMMUTABLE_TABLENAME + "  WHERE identifier = :identifier ;", consistency);
    }

    /**
     * @param id the {@link IRI} of the resource, the immutable data of which is to be retrieved
     * @return the RDF retrieved
     */
    public CompletionStage<Dataset> execute(IRI id) {
        return quads(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
