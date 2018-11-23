# trellis-cassandra
The rich, delicious ice cream of [Trellis LDP](https://github.com/trellis-ldp/trellis) laced with tasty [Apache Cassandra](https://cassandra.apache.org/) ribbons.

1. Clean separation of mutable and immutable (e.g. audit) data in separate tables.
2. Storage of RDF in the standard and easily-parsed [N-Quads](https://www.w3.org/TR/n-quads/) serialization.
3. The renowned distribution and scaling characteristics of Apache Cassandra.

[![CircleCI](https://circleci.com/gh/ajs6f/trellis-cassandra/tree/master.svg?style=svg)](https://circleci.com/gh/ajs6f/trellis-cassandra/tree/master)
[![Travis-CI](https://travis-ci.org/ajs6f/trellis-cassandra.svg?branch=master)](https://travis-ci.org/ajs6f/trellis-cassandra)

Use
```
mvn clean install
```
to build with a bundled Cassandra instance for testing. See Maven profiles in `webapp/pom.xml` for packaging options. Use
```
mvn -Dcassandra.skip -Dcassandra.contactAddress=$NODE -Dcassandra.nativeTransportPort=$PORT clean install
```
 to use an non-bundled Cassandra cluster for testing, but be aware that you must load an appropriate schema yourself if you do this. Please find an example in `src/test/resources/load.cql`.

To configure for runtime, provide the location and port of a contact node in your Cassandra cluster. This can be done via environment properties (or Java system properties). Use the names `CASSANDRA_CONTACT_PORT`(`cassandra.contactPort`) and `CASSANDRA_CONTACT_ADDRESS`(`cassandra.contactAddress`) (subject to change < 1.0). Additionally, you may configure the size of chunk (in bytes) used for binary storage as `CASSANDRA_MAX_CHUNK_SIZE`(`cassandra.maxChunkSize`).

You may also use a JSON document for these settings. Use `TRELLIS_CONFIG_FILE` (`configurationFile`) to use a file or `TRELLIS_CONFIG_URL` (`configurationUrl`) to use a document loaded from an arbitrary URL. The document should be a simple object with keys named as system properties shown above (`cassandra.contactPort`, `cassandra.contactAddress`, etc.).

