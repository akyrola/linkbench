package neo4j.twitter;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.*;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * @author Aapo Kyrola
 */
public class Neo4jTwitter {
    private static final String DB_PATH = "/Users/akyrola/bin/neo4j-enterprise-2.0.0/data/graph.db_twitter/";
    GraphDatabaseService graphDb;
    static int maxVertexId = 40000000;

    public Neo4jTwitter() {
        graphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( DB_PATH )
                .loadPropertiesFromFile( "/Users/akyrola/bin/neo4j-enterprise-2.0.0/conf/" + "neo4j.properties" ).newGraphDatabase();
        registerShutdownHook(graphDb);
    }

    private static enum RelTypes implements RelationshipType
    {
        FOLLOWS
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

    public void runShortestPath(int n) throws Exception {
        long stTime = System.currentTimeMillis();
        Transaction tr = graphDb.beginTx();

        SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMDD_HHmmss");
        String id = InetAddress.getLocalHost().getHostName().substring(0,8)  + "_NEO4J_" + sdf.format(new Date()) + "_" + n;


        BufferedWriter brw = new BufferedWriter(new FileWriter("/Users/akyrola/Projects/GraphCHI/GraphChi-DB/graphchiDB-java/twitter_" + id + "_shortest.txt"));


        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                PathExpanders.forTypeAndDirection(RelTypes.FOLLOWS, Direction.OUTGOING), 5);
        try {
            Random r = new Random(260379);
            for(int i=0; i < n; i++) {
                try {
                    long st = System.nanoTime();
                    long from =  Math.abs(r.nextLong() % 40000000) + 1;
                    long to =  Math.abs(r.nextLong() % 40000000) + 1;
                    Iterable<Path> paths = finder.findAllPaths(graphDb.getNodeById(from), graphDb.getNodeById(to));
                    Iterator<Path> it = paths.iterator();

                    int length = (-1);
                    long tt;
                    if (it.hasNext()) {
                        Path path = it.next();
                        tt = System.nanoTime() - st;
                        System.out.println(from + "," + to + "," + path.length() + " : " + tt*0.001 + " micros");
                        length = path.length();
                    } else {
                        tt = System.nanoTime() - st;

                        System.out.println(from + "," + to + "," + (-1) + " : " + tt*0.001 + " micros");
                    }
                    brw.write(length + "," + (tt * 0.001) + "\n");
                    brw.flush();
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }
        } finally {
            tr.close();
        }
        System.out.println("Total time " + (System.currentTimeMillis() - stTime) + " ms for " + n + " queries");
    }
    public void runFof(int n) throws IOException {
        Transaction tr = graphDb.beginTx();
        try {

            long t = System.currentTimeMillis();

            SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMDD_HHmmss");
            String id = InetAddress.getLocalHost().getHostName().substring(0,8)  + "_NEO4J_" + sdf.format(new Date()) + "_" + n;


            BufferedWriter brw = new BufferedWriter(new FileWriter("/Users/akyrola/Projects/GraphCHI/GraphChi-DB/graphchiDB-java/twitter_" + id + "_fof_limit200.txt"));
            brw.write("count,micros\n");
            Random r = new Random(260379);
            for(int i=0; i < n; i++) {
                try {
                    long v = Math.abs(r.nextLong() % 40000000) + 1;

                    final BitSet fofs = new BitSet(maxVertexId + 1);

                    long st = System.nanoTime();

                    Node nd = graphDb.getNodeById(v);

                    Iterator<Relationship> friends = nd.getRelationships(RelTypes.FOLLOWS, Direction.OUTGOING).iterator();

                    int j = 0;
                    while(friends.hasNext() && j < 200) {
                        j++;
                        Node frNode = friends.next().getEndNode();
                        Iterator<Relationship> fofIter = frNode.getRelationships(RelTypes.FOLLOWS, Direction.OUTGOING).iterator();
                        while(fofIter.hasNext()) {
                            fofs.set((int)fofIter.next().getEndNode().getId());
                        }
                    }


                    long tt = System.nanoTime() - st;
                    int cnt = fofs.cardinality();
                    if (cnt > 0) {
                        brw.write(cnt + "," + tt * 0.001 + "\n");
                    }
                    if (i % 100 == 0) {
                        brw.flush();
                        System.out.println((System.currentTimeMillis() - t) + "ms -- fof " + i + " / " + v + " --> " + cnt);
                    }
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }


            brw.close();
        } finally {
            tr.close();
        }
    }


    public static void main(String[] args) throws Exception {
        if (args[0].equals("insert")) {
            throw new IllegalArgumentException();
        } else if (args[0].equals("shortestpath")) {
            Neo4jTwitter bench = new Neo4jTwitter();
            bench.runShortestPath(Integer.parseInt(args[1]));
        } else if (args[0].equals("fof")) {
            Neo4jTwitter bench = new Neo4jTwitter();
            bench.runFof(Integer.parseInt(args[1]));
        }


    }
}
