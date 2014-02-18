package neo4j.linkbench;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Aapo Kyrola
 */
public class Neo4JBench {

    GraphDatabaseService graphDb;
    private static final String DB_PATH = "/Users/akyrola/bin/neo4j-enterprise-2.0.0/data/graph.db_/";

    public Neo4JBench() {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        registerShutdownHook(graphDb);
    }

    private static enum RelTypes implements RelationshipType
    {
        TYPE0,
        TYPE1
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }


    private static enum RelTypes2 implements RelationshipType
    {
        FOLLOWS
    }




    public void runFofLiveJournal() throws IOException {
        Transaction tr = graphDb.beginTx();
        try {

            long t = System.currentTimeMillis();

            BufferedWriter brw = new BufferedWriter(new FileWriter("neo4j_foflog_livej.txt"));
            Random r = new Random(260379);
            for(int i=1; i < 15001; i++) {
                long st = System.currentTimeMillis();
                long v = Math.abs(r.nextLong() % 4500000) + 1;
                Node nd = graphDb.getNodeById(v);
                Iterator<Relationship> friends = nd.getRelationships(RelTypes2.FOLLOWS, Direction.OUTGOING).iterator();


                HashSet<Long> fof = new HashSet<>();
                while(friends.hasNext()) {
                    Node frNode = friends.next().getEndNode();
                    Iterator<Relationship> fofIter = frNode.getRelationships(RelTypes2.FOLLOWS, Direction.OUTGOING).iterator();
                    while(fofIter.hasNext()) {
                        fof.add(fofIter.next().getEndNode().getId());
                    }
                }
                long tt = System.currentTimeMillis() - st;
                int cnt = fof.size();
                if (cnt > 1) {
                    brw.write(cnt + "\t" + tt + "\n");
                }
                if (i % 1000 == 0) {
                    System.out.println((System.currentTimeMillis() - t) + "ms -- fof " + i + " / " + v + " --> " + cnt);
                }

            }
            brw.close();
        } finally {
            tr.close();
        }
    }

    public void runFof() throws IOException {
        Transaction tr = graphDb.beginTx();
        try {

            long t = System.currentTimeMillis();

            BufferedWriter brw = new BufferedWriter(new FileWriter("neo4j_foflog.txt"));
            Random r = new Random(260379);
            for(int i=1; i < 50001; i++) {
                long st = System.currentTimeMillis();
                long v = Math.abs(r.nextLong() % 100000000) + 1;
                Node nd = graphDb.getNodeById(v);
                Iterator<Relationship> friends = nd.getRelationships(RelTypes.TYPE0, Direction.OUTGOING).iterator();


                HashSet<Long> fof = new HashSet<>();
                while(friends.hasNext()) {
                    Node frNode = friends.next().getEndNode();
                    Iterator<Relationship> fofIter = frNode.getRelationships(RelTypes.TYPE0, Direction.OUTGOING).iterator();
                    while(fofIter.hasNext()) {
                        fof.add(fofIter.next().getEndNode().getId());
                    }
                }
                long tt = System.currentTimeMillis() - st;
                int cnt = fof.size();
                if (cnt > 1) {
               //     brw.write(cnt + "\t" + tt + "\n");
                }
                if (i % 1000 == 0) {
                    System.out.println((System.currentTimeMillis() - t) + "ms -- fof " + i + " / " + v + " --> " + cnt);
                }

            }
            brw.close();
        } finally {
            tr.close();
        }
    }

    public static void main(String[] args) {
        try {
            Neo4JBench bench = new Neo4JBench();
            bench.runFof();

        } catch (Exception err) {
            err.printStackTrace();
        }
    }
}
