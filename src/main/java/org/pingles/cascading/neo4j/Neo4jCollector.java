package org.pingles.cascading.neo4j;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public interface Neo4jCollector {
    public Transaction beginTransaction();
    public Node createNode();
    public void addIndex(String nodeTypeName, String propertyName, Object propertyValue, Node node);


}
