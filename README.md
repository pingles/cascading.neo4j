# cascading.neo4j

Provides a Tap and Scheme suitable for sinking [Cascading](http://www.cascading.org) flows to a running Neo4j database. It currently doesn't support
sourcing data. It was built for use with Cascading 2.x and Hadoop 0.20.x.

Current build status: [![Build Status](https://secure.travis-ci.org/pingles/cascading.neo4j.png)](http://secure.travis-ci.org/pingles/cascading.neo4j)

It's a kind of work-in-progress: it works but seems much slower compared to Neo4j's batch processing. Given this, it
may or may not be suitable for your flows.

## Installing

cascading.neo4j is hosted on [Conjars.org](http://conjars.org): a repository for open-source Cascading libraries.

### Maven

```xml
<dependency>
    <groupId>org.pingles</groupId>
    <artifactId>cascading.neo4j</artifactId>
    <version>1.0</version>
</dependency>
<repositories>
    <repository>
        <id>conjars.org</id>
        <url>http://conjars.org/repo</url>
    </repository>
</repositories>
```

### Leiningen

Add the following to your `project.clj`:

```clojure
:dependencies [[org.pingles/cascading.neo4j "1.0"]
:repositories {"conjars.org" "http://conjars.org/repo"}
```

## Usage

`cascading.neo4j` provides 2 `Neo4jTap` classes- one suitable for use when running flows on Hadoop and one for local
flows. Given we're most interested in Hadoop flows the example below will be for that.

`cascading.neo4j` breaks flows into 2 parts- the creation of Nodes and Relationships. Note that nodes can be constructed
with their indexes at the same time. Creating a relationship between nodes requires looking up nodes via indexes.

### Sinking Nodes

Creating a `NodeScheme` requires you to provide the fields of the tuples that will flow to the Tap (via the Scheme). This
is so we can construct properties on the nodes with the correct names. Given we use Cascalog, we clean the field names
when creating properties to remove any ? or ! prefixes.

```java
Fields sourceFields = new Fields("name", "nationality");
NodeScheme scheme = new NodeScheme(sourceFields);
Tap sink = new Neo4jTap("http://neo4j.rest.co/db/data", scheme);
```

This tap can then be used in a regular Cascading flow. For example, the following shows reading from a delimited
file (containing name and nationality attributes). It will construct a Node for every record, and set `name` and
`nationality` properties on the node with the corresponding values for the record.

```java
HadoopPlatform platform = new HadoopPlatform();
Fields sourceFields = new Fields("name", "nationality");

Tap source = platform.getDelimitedFile(sourceFields, ",", filename);
NodeScheme scheme = new NodeScheme(sourceFields);
Tap sink = new Neo4jTap("http://neo4j.rest.co/db/data", scheme);

Pipe pipe = new Each("Flow nodes", sourceFields, new Identity());
Flow flow = platform.getFlowConnector().connect(source, sink, pipe);
flow.complete();
```

### Node Indexes

Creating a Neo4j index on a property is as simple as providing an `IndexSpec` instance to the Scheme. Following
the example above, to create an index on `name` you would create the scheme as follows:

```java
IndexSpec usersIndex = new IndexSpec("users", new Fields("name"));
NodeScheme scheme = new NodeScheme(new Fields("name", "nationality"), usersIndex);
```

Fields must intersect with the source fields (otherwise you'll generate errors trying to read attributes that don't exist).

### Sinking Relationships

Relationships connect nodes together. We therefore expect a flow to contain a source node key, a destination node key,
a label for the relationship, and any properties you want to add to the relationship.

For example, to connect a User node to a Nationality node your source data will look like this:

```
pingles,british,NATIONALITY
mjones,british,NATIONALITY
plam,canadian,NATIONALITY
t_bot,french,NATIONALITY
```

First field is the tail node, second is the head node, and third field is the relationship type. Graphically, it is represented in Cypher like this for the first row:

```
(pingles)-[:NATIONALITY]->(british)
```

You construct a `RelationshipScheme` instance providing the `IndexSpec` that will be used to identify the nodes. The
code below provides an example:

```java
IndexSpec usersIndex = new IndexSpec("users", new Fields("name"));
IndexSpec nationsIndex = new IndexSpec("nations", new Fields("name"));

Scheme scheme = new RelationshipScheme(new Fields("name", "nationality", "label"), fromIndex, toIndex);
Tap sink = new Neo4jTap("http://neo4j.rest.co/db/data", scheme);
```

In the example above, you can add more properties to the relationships by adding more Fields in the source to the
Scheme. For example:

```java
Fields fields = new Fields("name", "nationality", "label", "prop1", "prop2");
Scheme scheme = new RelationshipScheme(fields, fromIndex, toIndex);
```

### Batch Insert Operations

The `Neo4jRecordWriter` will create a Neo4j `Transaction` around all writes. If you set `org.neo4j.rest.batch_transaction=true`
the REST client will batch inserts made within that transaction. We've done a little experimentation with this and it
seems to make flows slightly faster.

The default behaviour of a `BatchTransaction` is to collect all mutations into a single REST call. `Neo4jRecordWriter` will
use its own chunk size to help break apart large operations. This ensures that Hadoop can still monitor progress, and
tweaks the size of the operations- large sets of mutations will result in lots of serialization for the REST calls.

This can be configured through Hadoop's `JobConf` configuration, by setting `org.neo4j.rest.batch_transaction` and
`org.pingles.neo4j.batch_size` properties.

## TODO

* What to do WRT SinkMode.REPLACE etc. if the node already exist in index
* The local and hadoop versions do largely the same thing, but in different ways. It'd be nice to factor out as much as possible to reduce duplication.

## Licensing

cascading.neo4j is licensed under the Apache 2.0 open source license: [http://opensource.org/licenses/Apache-2.0](http://opensource.org/licenses/Apache-2.0).