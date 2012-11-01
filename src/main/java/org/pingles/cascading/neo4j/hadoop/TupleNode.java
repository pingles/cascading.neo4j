package org.pingles.cascading.neo4j.hadoop;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class TupleNode implements NodeWritable {
    private final TupleEntry tupleEntry;

    public TupleNode(TupleEntry tupleEntry) {
        this.tupleEntry = tupleEntry;
    }

    public void addNode(GraphDatabaseService service) {
        Transaction transaction = service.beginTx();

        try {
            Node node = service.createNode();
            Fields fields = tupleEntry.getFields();

            for (int idx = 0; idx < fields.size(); idx++) {
                String fieldName = (String) fields.get(idx);
                Object fieldValue = tupleEntry.getTuple().getObject(idx);

                node.setProperty(fieldName, fieldValue);
            }

            transaction.success();
        } finally {
            transaction.finish();
        }
    }
}
