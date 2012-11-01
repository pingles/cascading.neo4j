package org.pingles.cascading.neo4j.hadoop;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.pingles.cascading.neo4j.IndexSpec;

public class TupleNode implements NodeWritable {
    private final TupleEntry tupleEntry;
    private IndexSpec indexSpec;

    public TupleNode(TupleEntry tupleEntry) {
        this(tupleEntry, null);
    }

    public TupleNode(TupleEntry tupleEntry, IndexSpec indexSpec) {
        this.tupleEntry = tupleEntry;
        this.indexSpec = indexSpec;
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

            if (indexSpec != null) {
                Index<Node> index = service.index().forNodes(indexSpec.getNodeTypeName());

                Fields indexProperties = indexSpec.getIndexProperties();
                for (Comparable propName : indexProperties) {
                    index.add(node, propName.toString(), tupleEntry.getObject(propName));
                }
            }

            transaction.success();
        } finally {
            transaction.finish();
        }
    }
}
