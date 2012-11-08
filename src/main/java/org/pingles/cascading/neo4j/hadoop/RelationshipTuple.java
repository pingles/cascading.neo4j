package org.pingles.cascading.neo4j.hadoop;

import cascading.tuple.TupleEntry;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.pingles.cascading.neo4j.IndexSpec;
import org.pingles.cascading.neo4j.StringRelationshipType;

public class RelationshipTuple extends Neo4jTuple implements Neo4jWritable {
    private final IndexSpec fromIndexSpec;
    private final IndexSpec toIndexSpec;
    private final TupleEntry tupleEntry;

    public RelationshipTuple(IndexSpec fromIndex, IndexSpec toIndex, TupleEntry tupleEntry) {
        this.fromIndexSpec = fromIndex;
        this.toIndexSpec = toIndex;
        this.tupleEntry = tupleEntry;
    }

    public void store(GraphDatabaseService service) {
        Object fromKey = tupleEntry.getObject(0);
        Object toKey = tupleEntry.getObject(1);
        String relationshipLabel = tupleEntry.getString(2);

        try {
            Index<Node> fromIndex = getFromIndex(service);
            Index<Node> toIndex = getToIndex(service);

            Node fromNode = lookupNodeInIndex(fromIndex, fromIndexSpec.getFirstIndexPropertyName(), fromKey);
            Node toNode = lookupNodeInIndex(toIndex, toIndexSpec.getFirstIndexPropertyName(), toKey);

            Relationship relationship = fromNode.createRelationshipTo(toNode, new StringRelationshipType(relationshipLabel));

            if (tupleEntry.getFields().size() > 3) {
                for (int i = 3; i < tupleEntry.size(); i++) {
                    String propertyName = (String) tupleEntry.getFields().get(i);
                    Object propertyValue = tupleEntry.getObject(i);

                    if (propertyValue != null)      // would work too without but save a PUT request
                        relationship.setProperty(cleanPropertyName(propertyName), propertyValue);
                }
            }
        } catch (IndexLookupException e) {
            throw new RuntimeException(e);
        } catch (IndexDoesNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    private Node lookupNodeInIndex(Index<Node> index, String indexPropertyName, Object objectValue) throws IndexLookupException {
        String cleanPropertyName = cleanPropertyName(indexPropertyName);
        IndexHits<Node> nodes = index.get(cleanPropertyName, objectValue);

        if (nodes.size() == 0) {
            throw new IndexLookupException(index.getName(), cleanPropertyName, objectValue);
        }

        return nodes.getSingle();
    }

    private Index<Node> getToIndex(GraphDatabaseService service) throws IndexDoesNotExistException {
        return getNodeIndex(service, toIndexSpec.getNodeTypeName());
    }

    private Index<Node> getFromIndex(GraphDatabaseService service) throws IndexDoesNotExistException {
        return getNodeIndex(service, fromIndexSpec.getNodeTypeName());
    }

    private Index<Node> getNodeIndex(GraphDatabaseService service, String indexName) throws IndexDoesNotExistException {
        if (!service.index().existsForNodes(indexName)) {
            throw new IndexDoesNotExistException(indexName);
        }
        return service.index().forNodes(indexName);
    }

    public class IndexDoesNotExistException extends Exception {
        public IndexDoesNotExistException(String indexName) {
            super(String.format("No Node index named \"%s\" exists.", indexName));
        }
    }

    public class IndexLookupException extends Exception {
        private final String indexName;
        private final String propertyName;
        private final Object propertyValue;

        public IndexLookupException(String indexName, String propertyName, Object propertyValue) {
            this.indexName = indexName;
            this.propertyName = propertyName;
            this.propertyValue = propertyValue;
        }

        @Override
        public String getMessage() {
            return String.format("Couldn't lookup object in index \"%s\" identified by property \"%s\" with value \"%s\".", indexName, propertyName, propertyValue);
        }
    }
}
