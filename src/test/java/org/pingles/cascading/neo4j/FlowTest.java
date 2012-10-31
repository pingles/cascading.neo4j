package org.pingles.cascading.neo4j;

import cascading.flow.Flow;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.test.LocalPlatform;
import cascading.tuple.Fields;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static junit.framework.Assert.*;

@RunWith(JUnit4.class)
public class FlowTest extends Neo4jTest {
    @Test
    public void shouldSinkContentsToNeo() {
        LocalPlatform platform = new LocalPlatform();
        Tap sourceTap = platform.getTextFile(new Fields("name"), "src/test/resources/names.csv");

        Tap sinkTap = new Neo4jTap(new Neo4jNodeScheme(this.neo4j.getService()));
        Pipe pipe = new Each("Names", new Fields("name"), new Identity());

        Flow flow = platform.getFlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();

        assertEquals("pingles", neo4j.getNode(1).getProperty("name"));
        assertEquals("plam", neo4j.getNode(2).getProperty("name"));
    }

    @Test
    public void shouldCreateNodesAndIndexOnSpecifiedField() {
        LocalPlatform platform = new LocalPlatform();
        Tap sourceTap = platform.getTextFile(new Fields("name"), "src/test/resources/names.csv");

        Fields indexFields = new Fields("name");
        Tap sinkTap = new Neo4jTap(new Neo4jNodeScheme(this.neo4j.getService(), new IndexSpec("users", indexFields)));
        Pipe pipe = new Each("Names", new Fields("name"), new Identity());

        Flow flow = platform.getFlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();

        IndexHits<Node> nodes = neo4j.indexForNodes("users").get("name", "pingles");
        assertEquals(1, nodes.size());

        Node pinglesNode = nodes.getSingle();
        assertEquals("pingles", pinglesNode.getProperty("name"));
    }

    @Test
    public void shouldCreateRelationshipsBetweenNodes() {
        LocalPlatform platform = new LocalPlatform();
        Fields nodeIndexFields = new Fields("name");

        // import the nodes
        Tap nodeSourceTap = platform.getTextFile(new Fields("name"), "src/test/resources/names.csv");
        Tap nodeSinkTap = new Neo4jTap(new Neo4jNodeScheme(this.neo4j.getService(), new IndexSpec("users", nodeIndexFields)));
        Pipe nodePipe = new Each("Names", new Fields("name"), new Identity());
        Flow nodeFlow = platform.getFlowConnector().connect(nodeSourceTap, nodeSinkTap, nodePipe);
        nodeFlow.complete();

        // import relationships between previously inserted nodes
        Tap relationshipSourceTap = platform.getDelimitedFile(new Fields("from", "to", "relationshipType"), ",", "src/test/resources/relations.csv");
        Tap relationshipSinkTap = new Neo4jTap(new Neo4jRelationshipScheme(this.neo4j.getService(), new IndexSpec("users", nodeIndexFields)));
        Pipe relPipe = new Each("Relations", new Fields("from", "to", "relationshipType"), new Identity());
        Flow relFlow = platform.getFlowConnector().connect(relationshipSourceTap, relationshipSinkTap, relPipe);
        relFlow.complete();

        // lookup one of the nodes and check the association
        IndexHits<Node> nodes = neo4j.indexForNodes("users").get("name", "pingles");
        Node pinglesNode = nodes.getSingle();

        List<Relationship> relationships = toList(pinglesNode.getRelationships());
        assertEquals(1, relationships.size());
        assertEquals("plam", relationships.get(0).getEndNode().getProperty("name"));
    }

    @Test
    public void shouldCreateMultipleIndexes() {
        LocalPlatform platform = new LocalPlatform();
        Fields sourceFields = new Fields("name", "nationality");

        Tap sourceTap = platform.getDelimitedFile(sourceFields, ",", "src/test/resources/names_and_nationality.csv");

        Tap sinkTap = new Neo4jTap(new Neo4jNodeScheme(this.neo4j.getService(), new IndexSpec("users", sourceFields)));
        Pipe pipe = new Each("Names", sourceFields, new Identity());
        Flow flow = platform.getFlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();

        IndexHits<Node> nodes = neo4j.indexForNodes("users").get("nationality", "british");
        assertEquals(2, nodes.size());

        List<Node> nodeList = toList(nodes);
        assertEquals("pingles", nodeList.get(0).getProperty("name"));
        assertEquals("british", nodeList.get(0).getProperty("nationality"));
        assertEquals("angrymike", nodeList.get(1).getProperty("name"));
        assertEquals("british", nodeList.get(1).getProperty("nationality"));
    }

    static <T> List<T> toList(Iterable<T> c) {
        List<T> list = new ArrayList<T>();
        for (T o : c) {
            list.add(o);
        }
        return list;
    }
}
