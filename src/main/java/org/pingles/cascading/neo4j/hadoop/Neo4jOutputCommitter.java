package org.pingles.cascading.neo4j.hadoop;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class Neo4jOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) throws IOException {
    }

    @Override
    public void cleanupJob(JobContext jobContext) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    }
}
