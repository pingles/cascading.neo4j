package org.pingles.cascading.neo4j;

import cascading.flow.Flow;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.test.LocalPlatform;
import cascading.tuple.Fields;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.*;

@RunWith(JUnit4.class)
public class FlowTest extends Neo4jTest {

    private LocalPlatform localPlatform;

    @Before
    public void initializePlatform() {
        localPlatform = new LocalPlatform();
    }

    @Test
    public void shouldSinkContentsToNeo() {
        Tap sourceTap = localPlatform.getTextFile(new Fields("name"), "src/test/resources/names.csv");

        Tap sinkTap = new Neo4jTap(new Neo4jNodeScheme(this.neo4j.getService()));
        Pipe pipe = new Each("Names", new Fields("name"), new Identity());

        Flow flow = localPlatform.getFlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();

        assertEquals("pingles", neo4j.getNode(1).getProperty("name"));
        assertEquals("plam", neo4j.getNode(2).getProperty("name"));
    }

    @Test
    public void shouldCreateNodesAndIndexOnSpecifiedField() {
        Tap sourceTap = localPlatform.getTextFile(new Fields("name"), "src/test/resources/names.csv");

        Fields indexFields = new Fields("name");
        Tap sinkTap = new Neo4jTap(new Neo4jNodeScheme(this.neo4j.getService(), new IndexSpec("users", indexFields)));
        Pipe pipe = new Each("Names", new Fields("name"), new Identity());

        Flow flow = localPlatform.getFlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();

        IndexHits<Node> nodes = neo4j.indexForNodes("users").get("name", "pingles");
        assertEquals(1, nodes.size());

        Node pinglesNode = nodes.getSingle();
        assertEquals("pingles", pinglesNode.getProperty("name"));
    }

    @Test
    public void shouldCreateRelationshipsBetweenNodes() {
        Fields nodeIndexFields = new Fields("name");

        // import the nodes
        Tap nodeSourceTap = localPlatform.getTextFile(new Fields("name"), "src/test/resources/names.csv");
        Tap nodeSinkTap = new Neo4jTap(new Neo4jNodeScheme(this.neo4j.getService(), new IndexSpec("users", nodeIndexFields)));
        Pipe nodePipe = new Each("Names", new Fields("name"), new Identity());
        Flow nodeFlow = localPlatform.getFlowConnector().connect(nodeSourceTap, nodeSinkTap, nodePipe);
        nodeFlow.complete();

        // import relationships between previously inserted nodes
        Tap relationshipSourceTap = localPlatform.getDelimitedFile(new Fields("from", "to", "relationshipType"), ",", "src/test/resources/relations.csv");
        Tap relationshipSinkTap = new Neo4jTap(new Neo4jRelationshipScheme(this.neo4j.getService(), new IndexSpec("users", nodeIndexFields)));
        Pipe relPipe = new Each("Relations", new Fields("from", "to", "relationshipType"), new Identity());
        Flow relFlow = localPlatform.getFlowConnector().connect(relationshipSourceTap, relationshipSinkTap, relPipe);
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
        Fields sourceFields = new Fields("name", "nationality", "relationship");

        Tap sourceTap = localPlatform.getDelimitedFile(sourceFields, ",", "src/test/resources/names_and_nationality.csv");

        Tap sinkTap = new Neo4jTap(new Neo4jNodeScheme(this.neo4j.getService(), new IndexSpec("users", sourceFields)));
        Pipe pipe = new Each("Names", sourceFields, new Identity());
        Flow flow = localPlatform.getFlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();

        IndexHits<Node> nodes = neo4j.indexForNodes("users").get("nationality", "british");
        assertEquals(2, nodes.size());

        List<Node> nodeList = toList(nodes);
        assertEquals("pingles", nodeList.get(0).getProperty("name"));
        assertEquals("british", nodeList.get(0).getProperty("nationality"));
        assertEquals("angrymike", nodeList.get(1).getProperty("name"));
        assertEquals("british", nodeList.get(1).getProperty("nationality"));
    }

    @Test
    public void shouldCreateRelationshipsBetweenDifferentNodeTypes() {
        Fields usersSourceFields = new Fields("name", "nationality");
        Fields usersOutFields = new Fields("name");
        flowNodes("Users", "src/test/resources/names_and_nationality.csv", usersSourceFields, usersOutFields, new IndexSpec("users", usersOutFields));

        Fields nationalitiesFields = new Fields("name");
        flowNodes("Nationalities", "src/test/resources/nationalities.csv", nationalitiesFields, nationalitiesFields, new IndexSpec("nationalities", nationalitiesFields));

        IndexSpec fromIndexSpec = new IndexSpec("users", new Fields("name"));
        IndexSpec toIndexSpec = new IndexSpec("nationalities", new Fields("name"));
        flowRelations("Relations", "src/test/resources/names_and_nationality.csv", new Fields("person", "country", "relationship"), fromIndexSpec, toIndexSpec);

        IndexHits<Node> britishNodes = neo4j.indexForNodes("nationalities").get("name", "british");
        Node britishNode = britishNodes.getSingle();
        assertEquals(2, toList(britishNode.getRelationships()).size());

        Node plamNode = neo4j.indexForNodes("users").get("name", "plam").getSingle();
        assertNotNull(plamNode);
        assertEquals("plam", plamNode.getProperty("name"));
        List<Relationship> relationships = toList(plamNode.getRelationships());
        assertEquals(1, relationships.size());
        assertEquals("NATIONALITY", relationships.get(0).getType().name());
    }

    @Test
    public void shouldCreateDirectionalRelationships() {
        Fields nameFields = new Fields("name");
        flowNodes("Users", "src/test/resources/names_and_nationality.csv", new Fields("name", "nationality"), nameFields, new IndexSpec("users", nameFields));
        flowNodes("Nationalities", "src/test/resources/nationalities.csv", nameFields, nameFields, new IndexSpec("nationalities", nameFields));
        IndexSpec fromIndexSpec = new IndexSpec("users", nameFields);
        IndexSpec toIndexSpec = new IndexSpec("nationalities", nameFields);
        flowRelations("Relations", "src/test/resources/names_and_nationality.csv", new Fields("person", "country", "relationship"), fromIndexSpec, toIndexSpec);

        Node plamNode = neo4j.indexForNodes("users").get("name", "plam").getSingle();

        assertEquals(0, toList(plamNode.getRelationships(Direction.INCOMING)).size());
        assertEquals(1, toList(plamNode.getRelationships(Direction.OUTGOING)).size());
    }

    private void flowRelations(String name, String filename, Fields sourceFields, IndexSpec fromIndexSpec, IndexSpec toIndexSpec) {
        Tap relationshipSourceTap = localPlatform.getDelimitedFile(sourceFields, ",", filename);
        Tap relationshipSinkTap = new Neo4jTap(new Neo4jRelationshipScheme(this.neo4j.getService(), fromIndexSpec, toIndexSpec));
        Pipe relPipe = new Each(name, sourceFields, new Identity());
        Flow relFlow = localPlatform.getFlowConnector().connect(relationshipSourceTap, relationshipSinkTap, relPipe);
        relFlow.complete();
    }

    private void flowNodes(String name, String filename, Fields sourceFields, Fields outFields, IndexSpec indexSpec) {
        Tap nodeSourceTap = localPlatform.getDelimitedFile(sourceFields, ",", filename);
        Tap nodeSinkTap = new Neo4jTap(new Neo4jNodeScheme(neo4j.getService(), indexSpec));
        Pipe nodePipe = new Each(name, outFields, new Identity());
        Flow nodeFlow = localPlatform.getFlowConnector().connect(nodeSourceTap, nodeSinkTap, nodePipe);
        nodeFlow.complete();
    }

    static <T> List<T> toList(Iterable<T> c) {
        List<T> list = new ArrayList<T>();
        for (T o : c) {
            list.add(o);
        }
        return list;
    }
}
