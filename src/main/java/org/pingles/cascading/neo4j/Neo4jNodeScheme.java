package org.pingles.cascading.neo4j;

import cascading.flow.FlowProcess;
import cascading.scheme.ConcreteCall;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

import java.io.IOException;

/**
 * Allows the client to sink Nodes in the Neo4j database.
 */
public class Neo4jNodeScheme extends Scheme {
    private GraphDatabaseService service;
    private IndexSpec indexSpec;
    private Index<Node> index;

    public Neo4jNodeScheme(GraphDatabaseService service) {
        this.service = service;
    }

    public Neo4jNodeScheme(GraphDatabaseService service, IndexSpec indexSpec) {
        this.service = service;
        this.indexSpec = indexSpec;
        index = service.index().forNodes(indexSpec.getNodeTypeName());
    }

    @Override
    public void sourceConfInit(FlowProcess flowProcess, Tap tap, Object o) {
    }

    @Override
    public void sinkConfInit(FlowProcess flowProcess, Tap tap, Object o) {

    }

    @Override
    public boolean source(FlowProcess flowProcess, SourceCall sourceCall) throws IOException {
        Object key = sourceCall.getContext();
        return false;
    }

    @Override
    public void sink(FlowProcess flowProcess, SinkCall sinkCall) throws IOException {
        ConcreteCall call = (ConcreteCall) sinkCall;
        TupleEntry outgoingEntry = call.getOutgoingEntry();

        Transaction transaction = service.beginTx();
        try {
            Node node = service.createNode();

            Fields fields = outgoingEntry.getFields();

            for (int idx = 0; idx < fields.size(); idx++) {
                String fieldName = (String) fields.get(idx);
                Object fieldValue = outgoingEntry.getTuple().getObject(idx);

                node.setProperty(fieldName, fieldValue);
            }

            if (index != null) {
                String indexPropertyName = indexSpec.getIndexPropertyName();
                index.add(node, indexPropertyName, node.getProperty(indexPropertyName));
            }

            transaction.success();
        } finally {
            transaction.finish();
        }
    }
}
