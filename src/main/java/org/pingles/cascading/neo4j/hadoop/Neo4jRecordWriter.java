package org.pingles.cascading.neo4j.hadoop;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.neo4j.graphdb.Transaction;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class Neo4jRecordWriter<K, V extends Neo4jWritable> implements RecordWriter<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jRecordWriter.class);
    private RestGraphDatabase database;
    private Transaction transaction;
    private String restConnectionString;

    public Neo4jRecordWriter(String restConnectionString) {
        this.restConnectionString = restConnectionString;
        LOGGER.info("Creating Neo4jRecordWriter to connect to {}", restConnectionString);
    }

    public void write(K k, V v) throws IOException {
        if (transaction == null) {
            transaction = database().beginTx();
        }
        v.store(database);
    }

    public void close(Reporter reporter) throws IOException {
        if (transaction != null) {
            transaction.success();
            transaction.finish();
        }
        database().shutdown();
    }

    private RestGraphDatabase database() {
        if (database == null) {
            database = new RestGraphDatabase(restConnectionString);
        }
        return database;
    }
}
