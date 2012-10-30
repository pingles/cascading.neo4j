package org.pingles;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.List;

public class EmbeddedNeo4jService {

    private final GraphDatabaseService service;

    public EmbeddedNeo4jService() {
        service = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
    }

    public boolean isEmptyApartFromRootNode() {
        return numberOfNodes() == 1;
    }

    public long numberOfNodes() {
        return allNodes().size();
    }

    public List<Node> allNodes() {
        ArrayList<Node> nodes = new ArrayList<Node>();
        Iterable<Node> allNodes = service.getAllNodes();
        for (Node node : allNodes) {
            nodes.add(node);
        }
        return nodes;
    }

    public void shutdown() {
        service.shutdown();
    }

    public Transaction beginTransaction() {
        return service.beginTx();
    }

    public Node createNode() {
        return service.createNode();
    }

    public boolean nodeExistsInDatabase(Node node) {
        return service.getNodeById(node.getId()) != null;
    }
}
