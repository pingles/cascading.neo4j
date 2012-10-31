package org.pingles.cascading.neo4j;

import cascading.flow.FlowProcess;
import cascading.scheme.ConcreteCall;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

import java.io.IOException;

public class Neo4jRelationshipScheme extends Scheme {
    private final GraphDatabaseService service;
    private final IndexSpec fromIndexSpec;
    private final IndexSpec toIndexSpec;

    public Neo4jRelationshipScheme(GraphDatabaseService service, IndexSpec indexSpec) {
        this(service, indexSpec, indexSpec);
    }

    public Neo4jRelationshipScheme(GraphDatabaseService service, IndexSpec fromIndexSpec, IndexSpec toIndexSpec) {
        this.service = service;
        this.fromIndexSpec = fromIndexSpec;
        this.toIndexSpec = toIndexSpec;
    }

    @Override
    public void sourceConfInit(FlowProcess flowProcess, Tap tap, Object o) {
        throw new UnsupportedOperationException("Not yet");
    }

    @Override
    public boolean source(FlowProcess flowProcess, SourceCall sourceCall) throws IOException {
        throw new UnsupportedOperationException("Not yet");
    }

    @Override
    public void sinkConfInit(FlowProcess flowProcess, Tap tap, Object o) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void sink(FlowProcess flowProcess, SinkCall sinkCall) throws IOException {
        ConcreteCall call = (ConcreteCall) sinkCall;
        TupleEntry outgoingEntry = call.getOutgoingEntry();

        Object node1Key = outgoingEntry.getObject(0);
        Object node2Key = outgoingEntry.getObject(1);
        String relationshipLabel = outgoingEntry.getString(2);

        Index<Node> fromIndex = service.index().forNodes(fromIndexSpec.getNodeTypeName());
        Index<Node> toIndex = service.index().forNodes(toIndexSpec.getNodeTypeName());

        Node fromNode = fromIndex.get(fromIndexSpec.getFirstIndexPropertyName(), node1Key).getSingle();
        Node toNode = toIndex.get(toIndexSpec.getFirstIndexPropertyName(), node2Key).getSingle();

        if (fromNode != null && toNode != null) {
            Transaction tx = service.beginTx();
            try {
                fromNode.createRelationshipTo(toNode, new StringRelationshipType(relationshipLabel));
                tx.success();
            } finally {
                tx.finish();
            }
        }
    }
}
