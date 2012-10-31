package org.pingles.cascading.neo4j;

import cascading.flow.FlowProcess;
import cascading.scheme.ConcreteCall;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;

/**
 * Allows the client to sink Nodes in the Neo4j database.
 */
public class Neo4jNodeScheme extends Neo4jScheme {
    private IndexSpec indexSpec;

    public Neo4jNodeScheme() {
    }

    public Neo4jNodeScheme(IndexSpec indexSpec) {
        this.indexSpec = indexSpec;
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

        Transaction transaction = getNeo4jCollector().beginTransaction();
        try {
            Node node = getNeo4jCollector().createNode();

            Fields fields = outgoingEntry.getFields();

            for (int idx = 0; idx < fields.size(); idx++) {
                String fieldName = (String) fields.get(idx);
                Object fieldValue = outgoingEntry.getTuple().getObject(idx);

                node.setProperty(fieldName, fieldValue);
            }

            if (indexSpec != null) {
                Fields indexProperties = indexSpec.getIndexProperties();
                for (Comparable propName : indexProperties) {
                    getNeo4jCollector().addIndex(indexSpec.getNodeTypeName(), propName.toString(), outgoingEntry.getObject(propName), node);
                }
            }

            transaction.success();
        } finally {
            transaction.finish();
        }
    }


    // These are the contract we have with some persistence store
    private void addIndex(Node node, String propertyName, Object propertyValue) {
        // index = service.index().forNodes(indexSpec.getNodeTypeName());
    }

    private Node createNode() {
        return null;
    }

    private Transaction beginTransaction() {
        return null;
    }
}
