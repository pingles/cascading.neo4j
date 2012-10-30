package org.pingles.cascading.neo4j;

import org.neo4j.graphdb.RelationshipType;

public class StringRelationshipType implements RelationshipType {
    private final String name;

    public StringRelationshipType(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }
}
