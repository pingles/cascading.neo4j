package org.pingles;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static junit.framework.Assert.assertTrue;

@RunWith(JUnit4.class)
public class Neo4jTest {
    private EmbeddedNeo4jService neo4j;

    @Before
    public void startNeo4j() {
        neo4j = new EmbeddedNeo4jService();
    }

    @After
    public void stopNeo4j() {
        if (neo4j != null) {
            neo4j.shutdown();
        }
    }

    @Test
    public void shouldHaveEmptyDatabase() {
        assertTrue("more than the root node", neo4j.isEmptyApartFromRootNode());
    }

    @Test
    public void shouldStoreNode() {
        Transaction tx = neo4j.beginTransaction();
        Node node = neo4j.createNode();
        tx.success();
        tx.finish();

        assertTrue(neo4j.nodeExistsInDatabase(node));
    }
}
