package org.pingles.cascading.neo4j.hadoop;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.neo4j.graphdb.Transaction;
import org.neo4j.rest.graphdb.RestGraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Neo4jRecordWriter<K, V extends Neo4jWritable> implements RecordWriter<K, V> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jRecordWriter.class);
    private RestGraphDatabase database;
    private Transaction transaction;
    private String restConnectionString;
    private int recordsInBatch = 0;
    private long numberOfCommittedTransactions = 0;
    private final int batchSize;

    public Neo4jRecordWriter(String restConnectionString, int batchSize) {
        this.restConnectionString = restConnectionString;
        this.batchSize = batchSize;

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Creating Neo4jRecordWriter to connect to {}", restConnectionString);
            LOGGER.info("Batch size: {}", getBatchSize());
        }
    }

    public void write(K k, V v) throws IOException {
        ensureTransaction();
        v.store(database);
        recordsInBatch++;
    }

    private void ensureTransaction() {
        if (shouldCreateNewTransaction()) {
            if (existingTransaction()) {
                commitTransaction();
            }
            transaction = database().beginTx();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(String.format("Created new transaction: %s", transaction.getClass().toString()));
            }
        }
    }

    public void close(Reporter reporter) throws IOException {
        if (existingTransaction()) {
            commitTransaction();
        }
        database().shutdown();

        reporter.incrCounter("org.pingles.cascading.neo4j", "Batch transactions committed", numberOfCommittedTransactions);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(String.format("Closing writer. Committed %d transactions.", numberOfCommittedTransactions));
        }
    }

    private boolean shouldCreateNewTransaction() {
        return transaction == null || recordsInBatch % getBatchSize() == 0;
    }

    private boolean existingTransaction() {
        return (transaction != null);
    }

    private void commitTransaction() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(String.format("Committing transaction with %d records.", recordsInBatch));
        }

        transaction.success();
        transaction.finish();

        numberOfCommittedTransactions++;
        recordsInBatch = 0;
    }

    private RestGraphDatabase database() {
        if (database == null) {
            database = new RestGraphDatabase(restConnectionString);
        }
        return database;
    }

    private int getBatchSize() {
        return batchSize;
    }
}
