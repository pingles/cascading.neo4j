# cascading.neo4j

Provides a Tap and Scheme suitable for sinking Cascading flows to a running Neo4j database.

Current build status: [![Build Status](https://secure.travis-ci.org/pingles/cascading.neo4j.png)](http://secure.travis-ci.org/pingles/cascading.neo4j)

## TODO

* empty or null property value should not be stored
* What to do WRT SinkMode.REPLACE etc. if the node already exist in index