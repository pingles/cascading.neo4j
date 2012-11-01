package org.pingles.cascading.neo4j.hadoop;

import org.neo4j.graphdb.GraphDatabaseService;

public interface Neo4jWritable {
    public void store(GraphDatabaseService service);
}
