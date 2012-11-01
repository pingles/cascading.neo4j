package org.pingles.cascading.neo4j.local;

import cascading.flow.Flow;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.test.LocalPlatform;
import cascading.tuple.Fields;
import org.neo4j.graphdb.GraphDatabaseService;
import org.pingles.cascading.neo4j.IndexSpec;
import org.pingles.cascading.neo4j.local.Neo4jNodeScheme;
import org.pingles.cascading.neo4j.local.Neo4jRelationshipScheme;
import org.pingles.cascading.neo4j.local.Neo4jTap;

import java.util.ArrayList;
import java.util.List;

public class Neo4jTestCase {
    protected LocalPlatform localPlatform;

    public static <T> List<T> toList(Iterable<T> c) {
        List<T> list = new ArrayList<T>();
        for (T o : c) {
            list.add(o);
        }
        return list;
    }

    protected void flowRelations(String name, String filename, Fields sourceFields, IndexSpec indexspec, GraphDatabaseService graphDatabaseService) {
        flowRelations(name, filename, sourceFields, indexspec, indexspec, graphDatabaseService);
    }

    protected void flowRelations(String name, String filename, Fields sourceFields, IndexSpec fromIndexSpec, IndexSpec toIndexSpec, GraphDatabaseService graphDatabaseService) {
        Tap relationshipSourceTap = localPlatform.getDelimitedFile(sourceFields, ",", filename);
        Tap relationshipSinkTap = new Neo4jTap(new Neo4jRelationshipScheme(graphDatabaseService, sourceFields, fromIndexSpec, toIndexSpec));
        flowThroughPipe(name, sourceFields, relationshipSourceTap, relationshipSinkTap);
    }

    protected void flowNodes(String name, String filename, Fields sourceFields, Fields outFields, GraphDatabaseService graphDatabaseService) {
        Tap nodeSourceTap = localPlatform.getDelimitedFile(sourceFields, ",", filename);
        Tap nodeSinkTap = new Neo4jTap(new Neo4jNodeScheme(graphDatabaseService));
        flowThroughPipe(name, outFields, nodeSourceTap, nodeSinkTap);
    }

    protected void flowNodes(String name, String filename, Fields sourceFields, Fields outFields, IndexSpec indexSpec, GraphDatabaseService graphDatabaseService) {
        Tap nodeSourceTap = localPlatform.getDelimitedFile(sourceFields, ",", filename);
        Tap nodeSinkTap = new Neo4jTap(new Neo4jNodeScheme(graphDatabaseService, indexSpec));
        flowThroughPipe(name, outFields, nodeSourceTap, nodeSinkTap);
    }

    private void flowThroughPipe(String name, Fields outFields, Tap nodeSourceTap, Tap nodeSinkTap) {
        Pipe nodePipe = new Each(name, outFields, new Identity());
        Flow nodeFlow = localPlatform.getFlowConnector().connect(nodeSourceTap, nodeSinkTap, nodePipe);
        nodeFlow.complete();
    }
}
