package org.pingles.cascading.neo4j.hadoop;

import cascading.flow.Flow;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.test.HadoopPlatform;
import cascading.tuple.Fields;
import com.googlecode.totallylazy.Callable1;
import com.googlecode.totallylazy.Sequence;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.pingles.cascading.neo4j.IndexSpec;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.googlecode.totallylazy.Sequences.sequence;
import static junit.framework.Assert.*;
import static org.neo4j.helpers.collection.Iterables.toList;

@RunWith(JUnit4.class)
public class FlowTest {
    public static final String NEO4J_DB_DIR = "target/neo4jdb";
    public static final String REST_CONNECTION_STRING = "http://localhost:7575/db/data";
    private WrappingNeoServerBootstrapper server;
    private HadoopPlatform hadoopPlatform;
    private GraphDatabaseService graphDatabaseService;

    @Before
    public void beforeEach() throws IOException {
        hadoopPlatform = new HadoopPlatform();

        FileUtils.deleteDirectory(new File(NEO4J_DB_DIR));
        GraphDatabaseAPI graphdb = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(NEO4J_DB_DIR)
                .newGraphDatabase();
        ServerConfigurator config = new ServerConfigurator( graphdb );
        config.configuration().setProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, 7575);
        server = new WrappingNeoServerBootstrapper(graphdb, config);
        server.start();
    }

    @After
    public void afterEach() {
        server.stop();
    }

    @Test
    public void shouldStoreNodes() {
        Fields sourceFields = new Fields("name");

        Tap nodeSourceTap = hadoopPlatform.getDelimitedFile(sourceFields, ",", "src/test/resources/names.csv");
        Tap nodeSinkTap = new Neo4jTap(REST_CONNECTION_STRING, new Neo4jNodeScheme());
        Pipe nodePipe = new Each("Nodes", sourceFields, new Identity());
        Flow nodeFlow = hadoopPlatform.getFlowConnector().connect(nodeSourceTap, nodeSinkTap, nodePipe);
        nodeFlow.complete();

        assertEquals(2 + 1, toList(neoService().getAllNodes()).size());
    }

    @Test
    public void shouldStoreNodeWithMultipleProperties() {
        Fields sourceFields = new Fields("name", "nationality", "relationshipLabel");

        Tap nodeSourceTap = hadoopPlatform.getDelimitedFile(sourceFields, ",", "src/test/resources/names_and_nationality.csv");
        Tap nodeSinkTap = new Neo4jTap(REST_CONNECTION_STRING, new Neo4jNodeScheme());
        Pipe nodePipe = new Each("Nodes", sourceFields, new Identity());
        Flow nodeFlow = hadoopPlatform.getFlowConnector().connect(nodeSourceTap, nodeSinkTap, nodePipe);

        nodeFlow.complete();

        Node node = neoService().getNodeById(1);
        assertEquals(1, node.getId());
        assertEquals("pingles", node.getProperty("name"));
        assertEquals("british", node.getProperty("nationality"));
    }

    @Test
    public void shouldStoreNodeWithIndexes() {
        Fields sourceFields = new Fields("name", "nationality", "relationshipLabel");

        Tap nodeSourceTap = hadoopPlatform.getDelimitedFile(sourceFields, ",", "src/test/resources/names_and_nationality.csv");
        Tap nodeSinkTap = new Neo4jTap(REST_CONNECTION_STRING, new Neo4jNodeScheme(new IndexSpec("users", new Fields("name", "nationality"))));
        Pipe nodePipe = new Each("Nodes", sourceFields, new Identity());
        Flow nodeFlow = hadoopPlatform.getFlowConnector().connect(nodeSourceTap, nodeSinkTap, nodePipe);

        nodeFlow.complete();

        List<Node> nodes = toList(neoService().index().forNodes("users").get("nationality", "british"));

        assertEquals(2, nodes.size());
        assertEquals("pingles", nodes.get(0).getProperty("name"));
        assertEquals("angrymike", nodes.get(1).getProperty("name"));
    }

    protected GraphDatabaseService neoService() {
        if (graphDatabaseService == null) {
            graphDatabaseService = new RestGraphDatabase(REST_CONNECTION_STRING);
        }
        return graphDatabaseService;
    }
}
