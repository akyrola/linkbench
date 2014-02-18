package neo4j.livejournal;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

/**
 * @author Aapo Kyrola
 */
public class Neo4jLIvejournal {
    private static final String DB_PATH = "/Users/akyrola/bin/neo4j-enterprise-2.0.0/data/graph.db_livejournal/";
    GraphDatabaseService graphDb;
    static int maxVertexId = 4600000;

    public Neo4jLIvejournal() {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );

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

        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                PathExpanders.forTypeAndDirection(RelTypes.FOLLOWS, Direction.OUTGOING), 5);
        try {
            Random r = new Random(260379);
            for(int i=0; i < n; i++) {
                try {
                    long st = System.nanoTime();
                    long from =  Math.abs(r.nextLong() % 4500000) + 1;
                    long to =  Math.abs(r.nextLong() % 4500000) + 1;
                    Iterable<Path> paths = finder.findAllPaths(graphDb.getNodeById(from), graphDb.getNodeById(to));
                    Iterator<Path> it = paths.iterator();

                    if (it.hasNext()) {
                        Path path = it.next();
                        long tt = System.nanoTime() - st;
                        System.out.println(from + "," + to + "," + path.length() + " : " + tt*0.001 + " micros");
                    } else {
                        long tt = System.nanoTime() - st;

                        System.out.println(from + "," + to + "," + (-1) + " : " + tt*0.001 + " micros");
                    }
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


            BufferedWriter brw = new BufferedWriter(new FileWriter("/Users/akyrola/Projects/GraphCHI/GraphChi-DB/graphchiDB-java/livejournal_" + id + "_fof_limit200.txt"));
            Random r = new Random(260379);
            for(int i=0; i < n; i++) {
                try {
                long v = Math.abs(r.nextLong() % 4500000) + 1;

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
                    brw.write(cnt + "," + tt * 0.001 + "," + v + "\n");
                }
                if (i % 1000 == 0) {
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


    private static void insertData() throws IOException {
        FileUtils.deleteRecursively(new File(DB_PATH));

        HashMap<String, String> config = new HashMap<String, String>();

        config.put("neostore.nodestore.db.mapped_memory", "90M");
        config.put("neostore.relationshipstore.db.mapped_memory", "3G");
        config.put("neostore.propertystore.db.mapped_memory", "50M");
        config.put("neostore.propertystore.db.strings.mapped_memory", "10M");
        config.put("neostore.nodestore.db.mapped_memory", "90M");
        config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");

        BatchInserter inserter = BatchInserters.inserter(DB_PATH, config);


        long t = System.currentTimeMillis();
        for(int i=0; i <maxVertexId; i++) {
            inserter.createNode(i, null);
            if (i % 1000000 == 0) {
                System.out.println((System.currentTimeMillis() - t) * 0.001 + " s., create node " + i);
            }
        }
        InputStream edgeInput = new FileInputStream("/Users/akyrola/graphs/soc-LiveJournal1.txt");
        BufferedReader edgeReader = new BufferedReader(new InputStreamReader(edgeInput));

        SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMDD_HHmmss");
        String id = InetAddress.getLocalHost().getHostName().substring(0,8)  + "_NEO4J_" + sdf.format(new Date());

        BufferedWriter insertLog =
                new BufferedWriter(new FileWriter("/Users/akyrola/Projects/GraphCHI/GraphChi-DB/graphchiDB-java/ingest_neo4j_livejournal_" + id + ".csv"));

        t = System.currentTimeMillis();
        long numEdges = 0;

        String ln;
        long last = System.currentTimeMillis();

        while ((ln = edgeReader.readLine()) != null) {
            if (!ln.startsWith("#") && ln.length() > 0) {
                String[] tok = ln.split("\t");
                if (tok.length == 1) tok = ln.split(" ");

                int from =  Integer.parseInt(tok[0]);
                int to = Integer.parseInt(tok[1]);

                if (from < maxVertexId && to < maxVertexId) {
                    inserter.createRelationship(from, to, RelTypes.FOLLOWS, null);
                }
                numEdges++;

                if (numEdges % 100000 == 0) {
                    insertLog.write((System.currentTimeMillis() - t) + "," + numEdges + "\n");
                    insertLog.flush();
                    System.out.println("Edges: " + numEdges + "; " +
                            (100000.0 / (0.001*(System.currentTimeMillis() - last))) + " edges/sec");
                    last = System.currentTimeMillis();
                }
            }
        }


        System.out.println("Shutting down inserter");
        inserter.shutdown();
    }

    public static void main(String[] args) throws Exception {
        if (args[0].equals("insert")) {
            insertData();
        } else if (args[0].equals("shortestpath")) {
            Neo4jLivejournal bench = new Neo4jLivejournal();
            bench.runShortestPath(Integer.parseInt(args[1]));
        } else if (args[0].equals("fof")) {
            Neo4jLivejournal bench = new Neo4jLivejournal();
            bench.runFof(Integer.parseInt(args[1]));
        }


    }

}
