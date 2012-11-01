package org.pingles.cascading.neo4j.hadoop;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public class Neo4jTap extends Tap<JobConf, RecordReader, OutputCollector> {
    private final String id = UUID.randomUUID().toString();
    private final String restConnectionString;

    public Neo4jTap(String restConnectionString, Scheme scheme) {
        super(scheme);
        this.restConnectionString = restConnectionString;
    }

    @Override
    public String getIdentifier() {
        return restConnectionString + id;
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess<JobConf> flowProcess, RecordReader recordReader) throws IOException {
        throw new UnsupportedOperationException("Only sinking supported");
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, JobConf conf) {
        Neo4jOutputFormat.setNeo4jConnectionString(conf, restConnectionString);
        super.sinkConfInit(flowProcess, conf);
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<JobConf> flowProcess, OutputCollector outputCollector) throws IOException {
        Neo4jTapCollector collector = new Neo4jTapCollector(flowProcess, this);
        collector.prepare();
        return collector;
    }

    @Override
    public boolean createResource(JobConf conf) throws IOException {
        return true;
    }

    @Override
    public boolean deleteResource(JobConf conf) throws IOException {
        return true;
    }

    @Override
    public boolean resourceExists(JobConf conf) throws IOException {
        return false;
    }

    @Override
    public long getModifiedTime(JobConf conf) throws IOException {
        return System.currentTimeMillis();
    }

    @Override
    public String toString()
    {
        return "Neo4jTap{" + restConnectionString + '}';
    }
}