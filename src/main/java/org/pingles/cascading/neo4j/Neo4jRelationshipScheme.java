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
    private final IndexSpec indexSpec;

    public Neo4jRelationshipScheme(GraphDatabaseService service, IndexSpec indexSpec) {
        this.service = service;
        this.indexSpec = indexSpec;
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

        Object node1Key = outgoingEntry.get(0);
        Object node2Key = outgoingEntry.get(1);
        String relationshipLabel = (String) outgoingEntry.get(2);

        Index<Node> index = service.index().forNodes(indexSpec.getNodeTypeName());

        Node node1 = index.get(indexSpec.getIndexPropertyName(), node1Key).getSingle();
        Node node2 = index.get(indexSpec.getIndexPropertyName(), node2Key).getSingle();

        if (node1 != null && node2 != null) {
            Transaction tx = service.beginTx();
            try {
                node1.createRelationshipTo(node2, new StringRelationshipType(relationshipLabel));
                tx.success();
            } finally {
                tx.finish();
            }
        }
    }
}
