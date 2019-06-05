package edu.si.trellis;

import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.time.Instant;

import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.Resource;

abstract class CassandraBuildingService {

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

        return construct(id, ixnModel, hasAcl, binaryId, mimeType, container, modified);
    }

    abstract Resource construct(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryId, String mimeType,
                    IRI container, Instant modified);
}
