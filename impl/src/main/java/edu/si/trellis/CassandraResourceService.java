package edu.si.trellis;

import static java.time.Instant.now;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toCollection;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.Metadata.builder;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static org.trellisldp.api.TrellisUtils.TRELLIS_DATA_PREFIX;
import static org.trellisldp.vocabulary.LDP.BasicContainer;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.NonRDFSource;
import static org.trellisldp.vocabulary.LDP.RDFSource;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;

import edu.si.trellis.query.rdf.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.*;
import org.trellisldp.vocabulary.LDP;

/**
 * Implements persistence into a simple Apache Cassandra schema.
 *
 * @author ajs6f
 *
 */
public class CassandraResourceService implements ResourceService, MementoService {

    private static final ImmutableSet<IRI> SUPPORTED_INTERACTION_MODELS = ImmutableSet.of(LDP.Resource, RDFSource,
                    NonRDFSource, Container, BasicContainer);

    private static final Logger log = getLogger(CassandraResourceService.class);

    private final Delete delete;

    private final Get get;

    private final ImmutableInsert immutableInsert;

    private final MutableInsert mutableInsert;

    private final Mementos mementos;

    private final Touch touch;

    private final BasicContainment bcontainment;

    private final MutableRetrieve mutableRetrieve;

    private final ImmutableRetrieve immutableRetrieve;

    /**
     * Constructor.
     * 
     * @param delete the {@link Delete} query to use
     * @param get the {@link Get} query to use
     * @param immutableInsert the {@link ImmutableInsert} query to use
     * @param mutableInsert the {@link MutableInsert} query to use
     * @param mementos the {@link Mementos} query to use
     * @param touch the {@link Touch} query to use
     * @param mutableRetrieve the {@link MutableRetrieve} query to use
     * @param immutableRetrieve the {@link ImmutableRetrieve} query to use
     * @param bcontainment the {@link BasicContainment} query to use
     */
    @Inject
    public CassandraResourceService(Delete delete, Get get, ImmutableInsert immutableInsert,
                    MutableInsert mutableInsert, Mementos mementos, Touch touch, MutableRetrieve mutableRetrieve,
                    ImmutableRetrieve immutableRetrieve, BasicContainment bcontainment) {
        this.delete = delete;
        this.get = get;
        this.immutableInsert = immutableInsert;
        this.mutableInsert = mutableInsert;
        this.mementos = mementos;
        this.touch = touch;
        this.immutableRetrieve = immutableRetrieve;
        this.mutableRetrieve = mutableRetrieve;
        this.bcontainment = bcontainment;
    }

    /**
     * Build a root container.
     */
    @PostConstruct
    void initializeRoot() {
        IRI rootIri = TrellisUtils.getInstance().createIRI(TRELLIS_DATA_PREFIX);
        try {
            if (get(rootIri).get().equals(MISSING_RESOURCE)) {
                log.info("Building repository root...");
                Metadata rootResource = builder(rootIri).interactionModel(BasicContainer).build();
                create(rootResource, null).get();
                log.info("Done building repository root.");                
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedStartupException("Interrupted while building repository root!", e);
        } catch (ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }

    private Resource buildResource(ResultSet rows) {
        final Row metadata;
        if ((metadata = rows.one()) == null) {
            log.debug("Resource was not found");
            return MISSING_RESOURCE;
        }
        IRI id = metadata.get("identifier", IRI.class);
        log.debug("Resource {} was found, computing metadata.", id);
        IRI ixnModel = metadata.get("interactionModel", IRI.class);
        log.debug("Found interactionModel = {} for resource {}", ixnModel, id);
        boolean hasAcl = metadata.getBool("hasAcl");
        log.debug("Found hasAcl = {} for resource {}", hasAcl, id);
        IRI binaryId = metadata.get("binaryIdentifier", IRI.class);
        log.debug("Found binaryIdentifier = {} for resource {}", binaryId, id);
        String mimeType = metadata.getString("mimetype");
        log.debug("Found mimeType = {} for resource {}", mimeType, id);
        IRI container = metadata.get("container", IRI.class);
        log.debug("Found container = {} for resource {}", container, id);
        Instant modified = metadata.get("modified", Instant.class);
        log.debug("Found modified = {} for resource {}", modified, id);
        UUID created = metadata.getUUID("created");
        log.debug("Found created = {} for resource {}", created, id);
        return new CassandraResource(id, ixnModel, hasAcl, binaryId, mimeType, container, modified, created,
                        immutableRetrieve, mutableRetrieve, bcontainment);
    }

    @Override
    public CompletableFuture<? extends Resource> get(final IRI id) {
        return get(id, now());
    }

    @Override
    public CompletableFuture<Resource> get(final IRI id, Instant time) {
        log.debug("Retrieving: {} at {}", id, time);
        return get.execute(id, time).thenApply(this::buildResource);
    }

    @Override
    public String generateIdentifier() {
        return randomUUID().toString();
    }

    @Override
    public CompletableFuture<Void> add(final IRI id, final Dataset dataset) {
        log.debug("Adding immutable data to {}", id);
        return immutableInsert.execute(id, dataset, now());
    }

    @Override
    public CompletableFuture<Void> create(Metadata meta, Dataset data) {
        log.debug("Creating {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletableFuture<Void> replace(Metadata meta, Dataset data) {
        log.debug("Replacing {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletableFuture<Void> delete(Metadata meta) {
        log.debug("Deleting {}", meta.getIdentifier());
        return delete.execute(meta.getIdentifier());
    }

    /*
     * (non-Javadoc) TODO avoid read-modify-write?
     */
    @Override
    public CompletableFuture<Void> touch(IRI id) {
        return get(id).thenApply(resource -> ((CassandraResource) resource).getCreated())
                        .thenCompose(created -> touch.execute(now(), created, id));
    }

    @Override
    public CompletableFuture<Void> put(Resource resource) {
        // NOOP see our data model
        return completedFuture(null);
    }

    //@formatter:off
    @Override
    public CompletableFuture<SortedSet<Instant>> mementos(IRI id) {
        return mementos.execute(id).thenApply(
                        results -> results.all().stream()
                                        .map(row -> row.get("modified", Instant.class))
                                        .map(time -> time.truncatedTo(SECONDS))
                                        .collect(toCollection(TreeSet::new)));
    }
    //@formatter:on

    private CompletableFuture<Void> write(Metadata meta, Dataset data) {
        IRI id = meta.getIdentifier();
        IRI ixnModel = meta.getInteractionModel();
        IRI container = meta.getContainer().orElse(null);

        Optional<BinaryMetadata> binary = meta.getBinary();
        IRI binaryIdentifier = binary.map(BinaryMetadata::getIdentifier).orElse(null);
        String mimeType = binary.flatMap(BinaryMetadata::getMimeType).orElse(null);
        Instant now = now();

        return mutableInsert.execute(ixnModel, mimeType, now.truncatedTo(SECONDS), container, data, now,
                        binaryIdentifier, UUIDs.timeBased(), id);
    }

    @Override
    public Set<IRI> supportedInteractionModels() {
        return SUPPORTED_INTERACTION_MODELS;
    }
}
