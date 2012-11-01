package org.pingles.cascading.neo4j.hadoop;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Neo4jRecordWriter<K, V extends Neo4jWritable> implements RecordWriter<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jRecordWriter.class);
    private final RestGraphDatabase database;
    private final Reporter reporter;

    public Neo4jRecordWriter(String restConnectionString, Reporter reporter) {
        this.reporter = reporter;

        LOGGER.info("Creating Neo4jRecordWriter to connect to {}", restConnectionString);
        database = new RestGraphDatabase(restConnectionString);
    }

    public void write(K k, V v) throws IOException {
        v.store(database);
        reporter.incrCounter("org.pingles.cascading.Neo4j", "Nodes Created", 1);
    }

    public void close(Reporter reporter) throws IOException {
        database.shutdown();
    }
}
