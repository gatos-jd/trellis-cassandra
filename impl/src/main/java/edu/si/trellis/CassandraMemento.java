package edu.si.trellis;

import edu.si.trellis.query.rdf.ImmutableRetrieve;
import edu.si.trellis.query.rdf.MementoMutableRetrieve;

import java.time.Instant;
import java.util.stream.Stream;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

/**
 * A Memento of a {@link CassandraResource}.
 */
class CassandraMemento extends CassandraResource {

    private final MementoMutableRetrieve mementoMutableRetrieve;

    CassandraMemento(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryIdentifier, String mimeType, IRI container,
                    Instant modified, ImmutableRetrieve immutable, MementoMutableRetrieve mementoMutableRetrieve,
                    Dataset dataset) {
        super(id, ixnModel, hasAcl, binaryIdentifier, mimeType, container, modified, dataset);
        this.mementoMutableRetrieve = mementoMutableRetrieve;
    }

    protected Stream<Quad> mutableQuads() {
        return mementoMutableRetrieve.execute(getIdentifier(), getModified());
    }

    protected Stream<Quad> basicContainmentQuads() {
        return Stream.empty();
    }
}
