package org.pingles.cascading.neo4j.hadoop;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Neo4jTapCollector extends TupleEntrySchemeCollector implements OutputCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jTapCollector.class);
    private final Reporter reporter = Reporter.NULL;

    private final FlowProcess<JobConf> flowProcess;
    private final Tap<JobConf, RecordReader, OutputCollector> tap;
    private final JobConf config;
    private RecordWriter writer;

    public Neo4jTapCollector(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader, OutputCollector> tap) {
        super(flowProcess, tap.getScheme());
        this.flowProcess = flowProcess;
        this.tap = tap;
        this.config = new JobConf(flowProcess.getConfigCopy());
    }

    @Override
    protected void prepare() {
        try {
            initialise();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        super.prepare();
    }

    private void initialise() throws IOException {
        tap.sinkConfInit(flowProcess, config);
        OutputFormat format = config.getOutputFormat();
        LOGGER.info("Output format class is {}", format.getClass().toString());

        writer = format.getRecordWriter(null, config, tap.getIdentifier(), reporter);

        sinkCall.setOutput(this);
    }

    @Override
    public void close() {
        try {
            writer.close(reporter);
        } catch (IOException e) {
            LOGGER.error("Error closing writer.", e);
            throw new TapException(e);
        } finally {
            super.close();
        }
    }

    public void collect(Object writableComparable, Object writable) throws IOException {
        HadoopFlowProcess hadoopProcess = (HadoopFlowProcess)flowProcess;
        hadoopProcess.getReporter().progress();
        writer.write(writableComparable, writable);
    }
}
