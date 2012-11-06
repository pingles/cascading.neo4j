package org.pingles.cascading.neo4j.hadoop;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.pingles.cascading.neo4j.IndexSpec;

import java.io.IOException;

// Scheme<Config, Input, Output, SourceContext, SinkContext
public class NodeScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
    private IndexSpec indexSpec;

    public NodeScheme(Fields sourceFields) {
        this(sourceFields, null);
    }

    public NodeScheme(Fields sourceFields, IndexSpec indexSpec) {
        super(sourceFields);
        if (indexSpec != null)
            this.indexSpec = indexSpec;
    }

    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean source(FlowProcess flowProcess, SourceCall sourceCall) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(Neo4jOutputFormat.class);
    }

    @Override
    public void sink(FlowProcess flowProcess, SinkCall sinkCall) throws IOException {
        OutputCollector collector = (OutputCollector) sinkCall.getOutput();
        TupleEntry tuple = sinkCall.getOutgoingEntry();

        NodeTuple node;
        if (indexSpec != null) {
            node = new NodeTuple(getSourceFields(), tuple, indexSpec);
        } else {
            node = new NodeTuple(getSourceFields(), tuple);
        }

        collector.collect(Tuple.NULL, node);

        flowProcess.increment("org.pingles.cascading.Neo4j", "Nodes Created", 1);
    }
}
