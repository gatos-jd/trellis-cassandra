package edu.si.trellis;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.jena.riot.Lang.NQUADS;
import static org.apache.jena.riot.RDFDataMgr.read;
import static org.apache.jena.riot.RDFDataMgr.writeQuads;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.jena.JenaRDF;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RiotException;

class DatasetCodec implements TypeCodec<Dataset> {
    
    private static final GenericType<Dataset> TYPE_OF_DATASET = GenericType.of(Dataset.class);

    static final DatasetCodec datasetCodec = new DatasetCodec();

    private static final JenaRDF rdf = new JenaRDF();
    
    @Override
    public ByteBuffer encode(Dataset dataset, ProtocolVersion protocolVersion) {
        if (dataset == null || dataset.size() == 0) return null;
        return ByteBuffer.wrap(toNQuads(dataset));
    }

    private byte[] toNQuads(Dataset dataset) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
            writeQuads(bytes, dataset.stream().map(rdf::asJenaQuad).iterator());
            return bytes.toByteArray();
        } catch (RiotException e) {
            throw new IllegalArgumentException("Dataset is impossible to serialize!", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Dataset could not be serialized!", e);
        } 
    }

    @Override
    public Dataset decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null ? rdf.createDataset() : fromNQuads(Bytes.getArray(bytes));
    }

    private Dataset fromNQuads(byte[] bytes) {
        org.apache.jena.query.Dataset dataset = DatasetFactory.create();
        try {
            read(dataset, new ByteArrayInputStream(bytes), null, NQUADS);
            return rdf.asDataset(dataset);
        } catch (RiotException e) {
            throw new IllegalArgumentException("Dataset is impossible to deserialize!", e);
        }
    }

    @Override
    public Dataset parse(String quads) {
        if (quads == null || quads.isEmpty()) return rdf.createDataset();
        return fromNQuads(quads.getBytes(UTF_8));
    }

    @Override
    public String format(Dataset dataset) {
        if (dataset == null || dataset.size() == 0) return null;
        return new String(toNQuads(dataset), UTF_8);
    }

    @Override
    public GenericType<Dataset> getJavaType() {
        return TYPE_OF_DATASET;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.TEXT;
    }
}
