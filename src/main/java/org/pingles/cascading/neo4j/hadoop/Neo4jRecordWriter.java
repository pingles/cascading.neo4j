package org.pingles.cascading.neo4j.hadoop;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Neo4jRecordWriter<K, V extends TupleNode> implements RecordWriter<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jRecordWriter.class);
    private final RestGraphDatabase database;

    public Neo4jRecordWriter(String restConnectionString) {
        LOGGER.info("Creating Neo4jRecordWriter to connect to {}", restConnectionString);
        database = new RestGraphDatabase(restConnectionString);
    }

    public void write(K k, V v) throws IOException {
        LOGGER.info("Writing {}: {}", k, v);
        v.addNode(database);
    }

    public void close(Reporter reporter) throws IOException {
        database.shutdown();
    }
}
