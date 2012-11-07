package org.pingles.cascading.neo4j.hadoop;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

@RunWith(JUnit4.class)
public class Neo4jOutputFormatTest {
    @Test
    public void shouldSetSystemPropertyForNeo4jProperty() throws IOException {
        JobConf conf = new JobConf();
        Neo4jOutputFormat format = new Neo4jOutputFormat();

        conf.set("org.neo4j.rest.batch_transaction", "true");

        format.getRecordWriter(null, conf, null, null);

        assertEquals("true", System.getProperty("org.neo4j.rest.batch_transaction"));
    }
}
