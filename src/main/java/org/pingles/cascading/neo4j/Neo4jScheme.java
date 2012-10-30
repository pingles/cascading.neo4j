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

import java.io.IOException;
import java.util.Iterator;

public class Neo4jScheme extends Scheme {
    private GraphDatabaseService service;

    public Neo4jScheme(GraphDatabaseService service) {
        this.service = service;
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
            Node n = service.createNode();

            Fields fields = outgoingEntry.getFields();

            for (int idx = 0; idx < fields.size(); idx++) {
                String fieldName = (String) fields.get(idx);
                Object fieldValue = outgoingEntry.getTuple().getObject(idx);

                n.setProperty(fieldName, fieldValue);
            }

            transaction.success();
        } finally {
            transaction.finish();
        }
    }
}
