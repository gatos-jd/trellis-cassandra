# trellis-cassandra
Trellis LDP with Apache Cassandra


`mvn clean install` to build with a bundled Cassandra instance for testing and see Maven profiles in `webapp/pom.xml` for packaging options. Use `mvn -P-self-cassandra -Dcassandra.contactHost=$NODE -Dcassandra.nativeTransportPort=$PORT clean install` to use an non-bundled Cassandra cluster for testing, but be aware that you must load an appropriate schema yourself if you do this. Please find an example in `src/test/resources/load.cql`.

To configure for runtime, provide the location and port of a contact node in your Cassandra cluster. This can be done via environment properties or Java system properties (further methods coming soon). Use the names `cassandra.contactPort` and `cassandra.contactAddress` (subject to change < 1.0).


See [Trellis-Cassandra](https://github.com/ajs6f/trellis-cassandra) and [Trellis](https://github.com/trellis-ldp/trellis).


[![Travis-CI Status](https://travis-ci.org/ajs6f/trellis-cassandra.svg?branch=master)](https://travis-ci.org/ajs6f/trellis-cassandra)
