package org.pingles.cascading.neo4j.local;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;

import java.io.IOException;

public class Neo4jTap extends Tap {
    private final Scheme neo4jScheme;

    public Neo4jTap(Scheme scheme) {
        super(scheme, SinkMode.REPLACE);
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
        return new TupleEntrySchemeCollector(flowProcess, neo4jScheme);
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
