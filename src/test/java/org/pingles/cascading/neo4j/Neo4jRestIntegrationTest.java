package org.pingles.cascading.neo4j;

import cascading.test.LocalPlatform;
import cascading.tuple.Fields;
import org.apache.commons.io.FileUtils;
import org.junit.After;
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

import static junit.framework.Assert.assertEquals;

@RunWith(JUnit4.class)
public class Neo4jRestIntegrationTest extends Neo4jTestCase {
    public static final String NEO4J_DB_DIR = "target/neo4jdb";
    private WrappingNeoServerBootstrapper server;

    @Before
    public void startNeo4jRestServer() {
        localPlatform = new LocalPlatform();
        GraphDatabaseAPI graphdb = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(NEO4J_DB_DIR)
                .newGraphDatabase();
        ServerConfigurator config = new ServerConfigurator( graphdb );
        config.configuration().setProperty(Configurator.WEBSERVER_PORT_PROPERTY_KEY, 7575);
        server = new WrappingNeoServerBootstrapper(graphdb, config);
        server.start();
    }

    @Test
    public void shouldPersistNodesInRestService() {
        GraphDatabaseService service = new RestGraphDatabase("http://localhost:7575/db/data");

        Fields userFields = new Fields("name");
        flowNodes("Users", "src/test/resources/names.csv", userFields, userFields, service);

        assertEquals(2 + 1, toList(service.getAllNodes()).size());
        assertEquals("pingles", service.getNodeById(1).getProperty("name"));
        assertEquals("plam", service.getNodeById(2).getProperty("name"));
    }

    @After
    public void stopNeo4jRestServer() throws IOException {
        server.stop();
        FileUtils.deleteDirectory(new File(NEO4J_DB_DIR));
    }
}
