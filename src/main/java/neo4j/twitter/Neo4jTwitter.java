package neo4j.twitter;

import com.facebook.LinkBench.Phase;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.*;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.zip.GZIPInputStream;

/**
 * @author Aapo Kyrola
 */
public class Neo4jTwitter {
    private static final String DB_PATH = "/Users/akyrola/bin/neo4j-enterprise-2.0.0/data/graph.db_twitter/";

    private static enum RelTypes implements RelationshipType
    {
        FOLLOWS
    }



    public static void main(String[] args) throws Exception {
        FileUtils.deleteRecursively(new File(DB_PATH));

        HashMap<String, String> config = new HashMap<String, String>();

        config.put("neostore.nodestore.db.mapped_memory", "90M");
        config.put("neostore.relationshipstore.db.mapped_memory", "3G");
        config.put("neostore.propertystore.db.mapped_memory", "50M");
        config.put("neostore.propertystore.db.strings.mapped_memory", "10M");
        config.put("neostore.nodestore.db.mapped_memory", "90M");
        config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");

        BatchInserter inserter = BatchInserters.inserter(DB_PATH, config);

        int maxVertexId = 69000000;

        long t = System.currentTimeMillis();
        for(int i=1; i <maxVertexId; i++) {
            inserter.createNode(i, null);
            if (i % 1000000 == 0) {
                System.out.println((System.currentTimeMillis() - t) * 0.001 + " s., create node " + i);
            }
        }
        InputStream edgeInput =new GZIPInputStream(new FileInputStream("/Users/akyrola/graphs/twitter_rv.net.gz"));
        BufferedReader edgeReader = new BufferedReader(new InputStreamReader(edgeInput));

        SimpleDateFormat sdf = new java.text.SimpleDateFormat("YYYYMMDD_HHmmss");
        String id = InetAddress.getLocalHost().getHostName().substring(0,8)  + "_NEO4J_" + sdf.format(new Date());

        BufferedWriter insertLog = new BufferedWriter(new FileWriter("/Users/akyrola/Projects/GraphCHI/GraphChi-DB/graphchiDB-java/Gingest_neo4j_twitter_" + id + ".csv"));

        t = System.currentTimeMillis();
        long numEdges = 0;

        String ln;
        long last = System.currentTimeMillis();

        while ((ln = edgeReader.readLine()) != null) {
            if (!ln.startsWith("#") && ln.length() > 0) {
                String[] tok = ln.split("\t");
                if (tok.length == 1) tok = ln.split(" ");

                int from = 1 + Integer.parseInt(tok[0]);
                int to = 1 + Integer.parseInt(tok[1]);

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

        /*
         serial insert

        Random r = new Random();
        for(int j=1; j<maxVertexId; j++) {
            int N = Math.abs(r.nextInt()) % 200 + 1;
            for(int k=1; k<N; k++) {
                inserter.createRelationship(j, j+k, RelTypes.FOLLOWS, null);
                numEdges++;

                if (numEdges % 100000 == 0) {
                    insertLog.write((System.currentTimeMillis() - t) + "," + numEdges + "\n");
                    insertLog.flush();
                    System.out.println("Edges: " + numEdges + "; " +
                            (100000.0 / (0.001*(System.currentTimeMillis() - last))) + " edges/sec");
                    last = System.currentTimeMillis();
                }

            }
        }    */


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down batch inserter...");
                long t = System.currentTimeMillis();
                System.out.println("Done...: took " + (System.currentTimeMillis() - t) + " ms");
            }
        });


    }
}
