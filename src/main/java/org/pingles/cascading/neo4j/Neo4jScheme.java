package org.pingles.cascading.neo4j;

import cascading.scheme.Scheme;

public abstract class Neo4jScheme extends Scheme {
    protected Neo4jCollector neo4jCollector;

    public void setNeo4jCollector(Neo4jCollector collector) {
        this.neo4jCollector = collector;
    }

    protected Neo4jCollector getNeo4jCollector() {
        return neo4jCollector;
    }
}
