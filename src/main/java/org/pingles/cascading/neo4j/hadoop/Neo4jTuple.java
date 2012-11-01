package org.pingles.cascading.neo4j.hadoop;

public abstract class Neo4jTuple {
    // Neo4j doesn't seem to like persisting properties that start with a question mark
    // (which happens when sinking data from Cascalog). For now, just strip any preceding
    // question marks
    protected static String cleanPropertyName(String fieldName) {
        if (startsWithCascalogCharacter(fieldName)) {
            return cleanPropertyName(fieldName.substring(1));
        }
        return fieldName;
    }

    private static boolean startsWithCascalogCharacter(String val) {
        return val.startsWith("?") || val.startsWith("!");
    }
}
