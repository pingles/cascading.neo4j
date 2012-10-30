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
import org.neo4j.graphdb.index.IndexHits;

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
}
