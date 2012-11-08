package org.pingles.cascading.neo4j.hadoop;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexHits;
import org.pingles.cascading.neo4j.IndexSpec;
import org.pingles.cascading.neo4j.local.EmbeddedNeo4jService;

import java.util.List;

import static junit.framework.Assert.assertEquals;
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
