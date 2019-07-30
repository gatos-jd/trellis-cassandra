package edu.si.trellis;

import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.PreferContainment;
import static org.trellisldp.vocabulary.LDP.contains;
import static org.trellisldp.vocabulary.LDP.getSuperclassOf;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import edu.si.trellis.query.rdf.BasicContainment;
import edu.si.trellis.query.rdf.ImmutableInsert;
import edu.si.trellis.query.rdf.ImmutableRetrieve;
import edu.si.trellis.query.rdf.MutableInsert;
import edu.si.trellis.query.rdf.Touch;

import java.time.Instant;
import java.util.Spliterator;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.trellisldp.api.Resource;
import org.trellisldp.vocabulary.LDP;

abstract class CassandraBuildingService {

    private final ImmutableRetrieve immutableRetrieve;

    private final BasicContainment bcontainment;

    public CassandraBuildingService(ImmutableRetrieve immutableRetrieve, BasicContainment bcontainment) {
        this.immutableRetrieve = immutableRetrieve;
        this.bcontainment = bcontainment;
    }

    Resource parse(AsyncResultSet rows, Logger log, IRI id) {
        final Row metadata;
        if ((metadata = rows.one()) == null) {
            log.debug("{} was not found.", id);
            return MISSING_RESOURCE;
        }

        log.debug("{} was found, computing metadata.", id);
        IRI ixnModel = metadata.get("interactionModel", IRI.class);
        log.debug("Found interactionModel = {} for resource {}", ixnModel, id);
        boolean hasAcl = metadata.getBoolean("hasAcl");
        log.debug("Found hasAcl = {} for resource {}", hasAcl, id);
        IRI binaryId = metadata.get("binaryIdentifier", IRI.class);
        log.debug("Found binaryIdentifier = {} for resource {}", binaryId, id);
        String mimeType = metadata.getString("mimetype");
        log.debug("Found mimeType = {} for resource {}", mimeType, id);
        IRI container = metadata.get("container", IRI.class);
        log.debug("Found container = {} for resource {}", container, id);
        Instant modified = metadata.getInstant("modified");
        log.debug("Found modified = {} for resource {}", modified, id);
        Dataset dataset = metadata.get("modified", Dataset.class);
        log.debug("Found mutable = {} for resource {}", dataset, id);
        CompletionStage<Dataset> datasetComputation = immutableQuads(id).thenApply(im -> {
            im.stream().forEach(dataset::add);
            return dataset;
        });
        if (Container.equals(ixnModel) || Container.equals(LDP.getSuperclassOf(ixnModel))) {
            datasetComputation = basicContainmentQuads(id).thenAccept(quads -> quads.stream().forEach(quads::add));
        }

        return construct(id, ixnModel, hasAcl, binaryId, mimeType, container, modified, dataset);
    }

    private CompletionStage<Dataset> immutableQuads(IRI id) {
        return immutableRetrieve.execute(id);
    }

    protected CompletionStage<Dataset> basicContainmentQuads(IRI id) {
        Spliterator<Row> rows = bcontainment.execute(id).spliterator();
        Stream<IRI> contained = StreamSupport.stream(rows, false).map(r -> r.get("contained", IRI.class));
        return null;

        // contained.distinct().map(c -> rdfFactory.createQuad(PreferContainment, getIdentifier(), contains, c))
        // .peek(t -> log.trace("Built containment quad: {}", t));
    }

    abstract Resource construct(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryId, String mimeType, IRI container,
                    Instant modified, Dataset dataset);
}
