package org.pingles.cascading.neo4j;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.IOException;

public class Neo4jTap extends Tap {
    private final GraphDatabaseService graphDatabaseService;
    private final Neo4jScheme neo4jScheme;

    public Neo4jTap(GraphDatabaseService graphDatabaseService, Neo4jScheme scheme) {
        super(scheme, SinkMode.REPLACE);
        this.graphDatabaseService = graphDatabaseService;
        this.neo4jScheme = scheme;
    }

    @Override
    public String getIdentifier() {
        return "Identifier goes here";
    }

    @Override
    public TupleEntryIterator openForRead(FlowProcess flowProcess, Object o) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess flowProcess, Object o) throws IOException {
        EmbeddedNeo4jServiceCollector collector = new EmbeddedNeo4jServiceCollector(flowProcess, graphDatabaseService, neo4jScheme);
        return collector;
        //return new TupleEntrySchemeCollector(flowProcess, neo4jScheme);
    }

    @Override
    public boolean createResource(Object o) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean deleteResource(Object o) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean resourceExists(Object o) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getModifiedTime(Object o) throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
