package org.pingles.cascading.neo4j.hadoop;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.lucene.index.IndexNotFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.pingles.cascading.neo4j.IndexSpec;
import org.pingles.cascading.neo4j.local.EmbeddedNeo4jService;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.pingles.cascading.neo4j.local.Neo4jTestCase.toList;

@RunWith(JUnit4.class)
public class RelationshipTupleTest {
    private EmbeddedNeo4jService service;
    private Transaction transaction;

    @Before
    public void startNeo4j() {
        this.service = new EmbeddedNeo4jService();
        this.transaction = service.beginTransaction();
    }

    @Test
    public void shouldStoreRelationship() {
        IndexSpec usersIndex = new IndexSpec("users", new Fields("name"));
        Node n1 = service.createNode();
        n1.setProperty("name", "pingles");
        Node n2 = service.createNode();
        n2.setProperty("name", "plam");
        service.indexForNodes("users").add(n1, "name", "pingles");
        service.indexForNodes("users").add(n2, "name", "plam");

        TupleEntry tupleEntry = new TupleEntry(new Fields("fromName", "toName", "label"), new Tuple("pingles", "plam", "COLLEAGUE"));
        RelationshipTuple relTuple = new RelationshipTuple(usersIndex, usersIndex, tupleEntry);

        relTuple.store(service.getService());

        Node pingles = service.indexForNodes("users").get("name", "pingles").getSingle();
        List<Relationship> relationships = toList(pingles.getRelationships());

        assertEquals(1, relationships.size());
        assertEquals("plam", relationships.get(0).getEndNode().getProperty("name"));
    }

    @Test
    public void shouldThrowExceptionIfIndexDoesNotExist() {
        IndexSpec indexSpec = new IndexSpec("noIndexHere", new Fields());
        RelationshipTuple relTuple = new RelationshipTuple(indexSpec, indexSpec, new TupleEntry(new Tuple("pingles", "plam", "COLLEAGUE")));

        try {
            relTuple.store(service.getService());
            fail("Should have thrown exception");
        } catch (Exception e) {
            assertEquals("org.pingles.cascading.neo4j.hadoop.RelationshipTuple$IndexDoesNotExistException: No Node index named \"noIndexHere\" exists.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfNodeDoesNotExistInIndex() {
        Node node = service.createNode();
        Index<Node> index = service.indexForNodes("indexName");
        index.add(node, "propertyName", "propertyValue");

        IndexSpec indexSpec = new IndexSpec("indexName", new Fields("propertyName"));
        RelationshipTuple relTuple = new RelationshipTuple(indexSpec, indexSpec, new TupleEntry(new Fields("fromName", "toName", "label"), new Tuple("pingles", "plam", "COLLEAGUE")));

        try {
            relTuple.store(service.getService());
            fail("Should have thrown exception");
        } catch (Exception e) {
            assertEquals("org.pingles.cascading.neo4j.hadoop.RelationshipTuple$IndexLookupException: Couldn't lookup object in index \"indexName\" identified by property \"propertyName\" with value \"pingles\".", e.getMessage());
        }
    }

    @After
    public void stopNeo4j() {
        if (transaction != null) {
            transaction.success();
            transaction.finish();
        }
        if (service != null) {
            service.shutdown();
        }
    }
}
