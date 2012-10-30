package org.pingles.cascading.neo4j;

import cascading.tuple.Fields;

public class IndexSpec {
    private final String indexName;
    private final Fields fields;

    public IndexSpec(String indexName, Fields fields) {
        this.indexName = indexName;
        this.fields = fields;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexPropertyName() {
        return (String) fields.get(0);
    }
}
