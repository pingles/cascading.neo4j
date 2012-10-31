package org.pingles.cascading.neo4j;

import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntrySchemeCollector;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class EmbeddedNeo4jServiceCollector extends TupleEntrySchemeCollector implements Neo4jCollector {
    private GraphDatabaseService graphDatabaseService;

    public EmbeddedNeo4jServiceCollector(FlowProcess flowProcess, GraphDatabaseService graphDatabaseService, Neo4jScheme neo4jScheme) {
        super(flowProcess, neo4jScheme);
        this.graphDatabaseService = graphDatabaseService;
    }

    public Transaction beginTransaction() {
        return graphDatabaseService.beginTx();
    }

    public Node createNode() {
        return graphDatabaseService.createNode();
    }

    public void addIndex(String nodeTypeName, String propertyName, Object propertyValue, Node node) {
        graphDatabaseService.index().forNodes(nodeTypeName).add(node, propertyName, propertyValue);
        // = service.index().forNodes(indexSpec.getNodeTypeName());
    }
}
