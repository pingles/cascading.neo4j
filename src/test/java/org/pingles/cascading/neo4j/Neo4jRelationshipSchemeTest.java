package org.pingles.cascading.neo4j;

import cascading.tuple.Fields;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import static junit.framework.Assert.*;

@RunWith(JUnit4.class)
public class Neo4jRelationshipSchemeTest {
    @Test
    public void shouldThrowExceptionWhenNotEnoughFieldsProvidedForRelationship() {
        // to draw a relationship we need:
        // from, to, relationship name
        // any less and we can't draw the relation
        try {
            new Neo4jRelationshipScheme(stubService(), new Fields("blah"), new IndexSpec("users", new Fields()));
        } catch (IllegalArgumentException e) {
            return;
        }
        fail("Didn't throw IllegalArgumentException");
    }

    @Test
    public void shouldNotThrowExceptionWhenFieldsSpecifiesEnough() {
        try {
            new Neo4jRelationshipScheme(stubService(), new Fields("fromName", "toName", "relationshipLabel"), new IndexSpec("users", new Fields()));
        } catch (IllegalArgumentException e) {
            fail(e.getMessage());
        }
    }

    public GraphDatabaseService stubService() {
        return new GraphDatabaseService() {
            public Node createNode() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public Node getNodeById(long l) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public Relationship getRelationshipById(long l) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public Node getReferenceNode() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public Iterable<Node> getAllNodes() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public Iterable<RelationshipType> getRelationshipTypes() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public void shutdown() {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            public Transaction beginTx() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public <T> TransactionEventHandler<T> registerTransactionEventHandler(TransactionEventHandler<T> tTransactionEventHandler) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(TransactionEventHandler<T> tTransactionEventHandler) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public KernelEventHandler registerKernelEventHandler(KernelEventHandler kernelEventHandler) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public KernelEventHandler unregisterKernelEventHandler(KernelEventHandler kernelEventHandler) {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }

            public IndexManager index() {
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        };
    }
}
