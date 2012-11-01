package org.pingles.cascading.neo4j.hadoop;

import cascading.tuple.TupleEntry;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.pingles.cascading.neo4j.IndexSpec;
import org.pingles.cascading.neo4j.StringRelationshipType;

public class TupleRelationship implements Neo4jWritable {
    private final IndexSpec fromIndexSpec;
    private final IndexSpec toIndexSpec;
    private final TupleEntry tupleEntry;

    public TupleRelationship(IndexSpec fromIndex, IndexSpec toIndex, TupleEntry tupleEntry) {
        this.fromIndexSpec = fromIndex;
        this.toIndexSpec = toIndex;
        this.tupleEntry = tupleEntry;
    }

    public void store(GraphDatabaseService service) {
        Object node1Key = tupleEntry.getObject(0);
        Object node2Key = tupleEntry.getObject(1);
        String relationshipLabel = tupleEntry.getString(2);

        Index<Node> fromIndex = service.index().forNodes(fromIndexSpec.getNodeTypeName());
        Index<Node> toIndex = service.index().forNodes(toIndexSpec.getNodeTypeName());

        Node fromNode = fromIndex.get(fromIndexSpec.getFirstIndexPropertyName(), node1Key).getSingle();
        Node toNode = toIndex.get(toIndexSpec.getFirstIndexPropertyName(), node2Key).getSingle();

        Transaction transaction = service.beginTx();
        try {
            fromNode.createRelationshipTo(toNode, new StringRelationshipType(relationshipLabel));
            transaction.success();
        } finally {
            transaction.finish();
        }
    }
}
