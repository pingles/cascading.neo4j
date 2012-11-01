package org.pingles.cascading.neo4j.hadoop;

import cascading.flow.Flow;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.test.HadoopPlatform;
import cascading.tuple.Fields;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;

import java.io.File;
import java.io.IOException;

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

    protected GraphDatabaseService neoService() {
        if (graphDatabaseService == null) {
            graphDatabaseService = new RestGraphDatabase(REST_CONNECTION_STRING);
        }
        return graphDatabaseService;
    }
}
