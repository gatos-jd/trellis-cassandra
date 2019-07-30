package edu.si.trellis;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.TrellisUtils.toDataset;

import com.datastax.oss.driver.api.core.uuid.Uuids;

import edu.si.trellis.query.rdf.GetMemento;
import edu.si.trellis.query.rdf.ImmutableRetrieve;
import edu.si.trellis.query.rdf.MementoMutableRetrieve;
import edu.si.trellis.query.rdf.Mementoize;
import edu.si.trellis.query.rdf.Mementos;

import java.time.Instant;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.MementoService;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;

/**
 * A {@link MementoService} that stores Mementos in a Cassandra table.
 *
 */
public class CassandraMementoService extends CassandraBuildingService implements MementoService {

    private static final Logger log = getLogger(CassandraMementoService.class);

    private final Mementos mementos;

    private final Mementoize mementoize;

    private final GetMemento getMemento;

    private ImmutableRetrieve immutableRetrieve;

    private MementoMutableRetrieve mementoMutableRetrieve;

    @Inject
    public CassandraMementoService(Mementos mementos, Mementoize mementoize, GetMemento getMemento,
                    MementoMutableRetrieve mementoMutableRetrieve, ImmutableRetrieve immutableRetrieve) {
       super(immutableRetrieve, bcontainment);
        this.mementos = mementos;
        this.mementoize = mementoize;
        this.getMemento = getMemento;
        this.mementoMutableRetrieve = mementoMutableRetrieve;
        this.immutableRetrieve = immutableRetrieve;
    }

    @Override
    public CompletionStage<Void> put(Resource r) {

        IRI id = r.getIdentifier();
        IRI ixnModel = r.getInteractionModel();
        IRI container = r.getContainer().orElse(null);
        Optional<BinaryMetadata> binary = r.getBinaryMetadata();
        IRI binaryIdentifier = binary.map(BinaryMetadata::getIdentifier).orElse(null);
        String mimeType = binary.flatMap(BinaryMetadata::getMimeType).orElse(null);
        Dataset data = r.dataset().stream().collect(toDataset());
        Instant modified = r.getModified();
        UUID creation = Uuids.timeBased();

        log.debug("Writing Memento for {} at time: {}", id, modified);
        return mementoize.execute(ixnModel, mimeType, container, data, modified, binaryIdentifier, creation, id);
    }

    // @formatter:off
    @Override
    public CompletionStage<SortedSet<Instant>> mementos(IRI id) {
        return mementos.execute(id)
                        .thenApply(r -> r.map(row -> row.get("modified", Instant.class)))
                        .thenApply(r -> r.map(time -> time.truncatedTo(SECONDS)))
                        .thenApply(instants -> {
                            SortedSet<Instant> results = new TreeSet<>();
                            do instants.currentPage().forEach(results::add);
                            while (instants.hasMorePages());
                            return results;
                        });
    }
    // @formatter:on

    @Override
    public CompletionStage<Resource> get(final IRI id, Instant time) {
        log.debug("Retrieving Memento for: {} at {}", id, time);
        return getMemento.execute(id, time).thenApply(result -> parse(result, log, id));
    }

    @Override
    Resource construct(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryId, String mimeType, IRI container,
                    Instant modified, Dataset dataset) {
        return new CassandraMemento(id, ixnModel, hasAcl, binaryId, mimeType, container, modified, immutableRetrieve,
                        mementoMutableRetrieve, dataset);
    }
}
