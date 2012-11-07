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
    private static final String DEFAULT_BATCH_SIZE = "10";
    public static final String BATCH_SIZE_PROPERTY_NAME = "org.pingles.neo4j.batch_size";

    public static void setNeo4jConnectionString(JobConf conf, String restConnectionString) {
        conf.set(REST_CONN_STRING_CONFIG_PROPERTY, restConnectionString);
    }

    public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progress) throws IOException {
        setNeo4jJavaClientSystemProperties(jobConf);

        String restConnectionString = jobConf.get(REST_CONN_STRING_CONFIG_PROPERTY);
        LOGGER.info("Creating RecordWriter, connecting to {}", restConnectionString);

        int batchSize = Integer.valueOf(jobConf.get(BATCH_SIZE_PROPERTY_NAME, DEFAULT_BATCH_SIZE));

        return new Neo4jRecordWriter(restConnectionString, batchSize);
    }

    private void setNeo4jJavaClientSystemProperties(JobConf jobConf) {
        setSystemPropertyFromJobConf(jobConf, "org.neo4j.rest.batch_transaction");
        setSystemPropertyFromJobConf(jobConf, "org.neo4j.rest.read_timeout");
        setSystemPropertyFromJobConf(jobConf, "org.neo4j.rest.connect_timeout");
        setSystemPropertyFromJobConf(jobConf, "org.neo4j.rest.stream");
        setSystemPropertyFromJobConf(jobConf, "org.neo4j.rest.logging_filter");
    }

    public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {
    }

    private void setSystemPropertyFromJobConf(JobConf jobConf, String propertyName) {
        String propertyValue = jobConf.get(propertyName);
        if (propertyValue != null) {
            System.setProperty(propertyName, propertyValue);
        }
    }
}
