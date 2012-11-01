package org.pingles.cascading.neo4j.hadoop;

import org.neo4j.graphdb.GraphDatabaseService;

public interface NodeWritable {
    public void addNode(GraphDatabaseService service);
}
