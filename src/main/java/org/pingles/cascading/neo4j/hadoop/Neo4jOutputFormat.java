package org.pingles.cascading.neo4j.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Neo4jOutputFormat<K, V> implements OutputFormat<K,V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jOutputFormat.class);
    public static final String REST_CONN_STRING_CONFIG_PROPERTY = "org.pingles.neo4j.connection.string";

    public static void setNeo4jConnectionString(JobConf conf, String restConnectionString) {
        conf.set(REST_CONN_STRING_CONFIG_PROPERTY, restConnectionString);
    }

    public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        String restConnectionString = jobConf.get(REST_CONN_STRING_CONFIG_PROPERTY);
        LOGGER.info("Creating RecordWriter, connecting to {}", restConnectionString);
        return new Neo4jRecordWriter(restConnectionString);
    }

    public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {
    }
}
