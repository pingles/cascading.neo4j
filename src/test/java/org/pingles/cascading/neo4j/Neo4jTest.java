package org.pingles.cascading.neo4j;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

public class Neo4jTest {
    protected EmbeddedNeo4jService neo4j;

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
}
