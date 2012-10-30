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

import static junit.framework.Assert.*;

@RunWith(JUnit4.class)
public class FlowTest extends Neo4jTest {
    @Test
    public void shouldSinkContentsToNeo() {
        LocalPlatform platform = new LocalPlatform();
        Tap sourceTap = platform.getTextFile(new Fields("name"), "src/test/resources/names.csv");

        Tap sinkTap = new Neo4jTap(new Neo4jScheme(this.neo4j.getService()));
        Pipe pipe = new Each("Names", new Fields("name"), new Identity());

        Flow flow = platform.getFlowConnector().connect(sourceTap, sinkTap, pipe);
        flow.complete();

        assertEquals("pingles", neo4j.getNode(1).getProperty("name"));
        assertEquals("plam", neo4j.getNode(2).getProperty("name"));
    }
}
