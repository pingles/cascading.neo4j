# cascading.neo4j

Provides a Tap and Scheme suitable for sinking Cascading flows to a running Neo4j database.

Current build status: [![Build Status](https://secure.travis-ci.org/pingles/cascading.neo4j.png)](http://secure.travis-ci.org/pingles/cascading.neo4j)

## TODO

* Allow relations between nodes from difference Indexes
* Ensure scheme can connect to remote Neo server
* Ensure relations fields are of the form: from-key, to-key, relationship-label
* Specify field names when persisting properties on Nodes. Will be useful given Cascalog prefixes with ? and !
* Allow relationships to carry properties

