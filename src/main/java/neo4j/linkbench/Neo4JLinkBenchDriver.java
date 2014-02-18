package neo4j.linkbench;

import com.facebook.LinkBench.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.facebook.LinkBench.Node;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


/**
 * Based on https://github.com/graphaware/linkbench-neo4j/
 * * @author Aapo Kyrola
 */

class Neo4JLinkBenchDriver extends GraphStore {
    static Neo4JLinkBenchDriverSingleton singleton = new Neo4JLinkBenchDriverSingleton();

    boolean debug = false;


    @Override
    public void initialize(Properties p, Phase currentPhase, int threadId) throws IOException, Exception {
        singleton.initialize(p, currentPhase, threadId);
    }

    @Override
    public void close() {
        singleton.close();
    }

    @Override
    public void clearErrors(int threadID) {
    }

    @Override
    public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
        if (debug) System.out.println("addLink");
        return singleton.addLink(dbid, a, noinverse);
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        if (debug) System.out.println("deleteLInk");
        return singleton.deleteLink(dbid, id1, link_type, id2, noinverse, expunge);
    }

    @Override
    public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
        if (debug)System.out.println("Updatelink");
        return singleton.updateLink(dbid, a, noinverse);
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        if (debug)System.out.println("getLink");
        return singleton.getLink(dbid, id1, link_type, id2);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        if (debug)System.out.println("GetlinkList");
        return singleton.getLinkList(dbid, id1, link_type);
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        if (debug)System.out.println("GetlinkList2");

        return singleton.getLinkList(dbid, id1, link_type, minTimestamp, maxTimestamp, offset, limit);
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        if (debug)System.out.println("countLinks");
        return singleton.countLinks(dbid, id1, link_type);
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {
        if (debug) System.out.println("reset");
        singleton.resetNodeStore(dbid, startID);
    }

    @Override
    public long addNode(String dbid, Node node) throws Exception {
        if (debug) System.out.println("addNode");
        return singleton.addNode(dbid, node);
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {
        if (debug) System.out.println("getNode");
        return singleton.getNode(dbid, type, id);
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        if (debug) System.out.println("updateNode");
        return singleton.updateNode(dbid, node);
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        if (debug)   System.out.println("deleteNode");
        return singleton.deleteNode(dbid, type, id);
    }
}


class Neo4JLinkBenchDriverSingleton extends GraphStore {
    private static final String DB_PATH = "/Users/akyrola/bin/neo4j-enterprise-2.0.0/data/graph.db/";

    private static enum RelTypes implements RelationshipType
    {
        TYPE0,
        TYPE1
    }

    private AtomicLong idSequence = new AtomicLong(1);


    //names of Neo4j properties
    protected static final String ID = "id";
    protected static final String TIME = "time";
    protected static final String TYPE = "type";
    protected static final String VERSION = "version";
    protected static final String DATA = "data";
    protected static final String VISIBILITY = "visibility";

    private BatchInserter inserter;
    private Phase currentPhase;
    private boolean initialized = false;

    private void clearDb()
    {
        try
        {
            FileUtils.deleteRecursively( new File( DB_PATH ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public synchronized void initialize(Properties p1, Phase currentPhase, int threadId) throws IOException, Exception {
        this.currentPhase = currentPhase;
        if (currentPhase == Phase.LOAD && !initialized) {

            clearDb();
            HashMap<String, String> config = new HashMap<String, String>();

            config.put("neostore.nodestore.db.mapped_memory", "90M");
            config.put("neostore.relationshipstore.db.mapped_memory", "3G");
            config.put("neostore.propertystore.db.mapped_memory", "50M");
            config.put("neostore.propertystore.db.strings.mapped_memory", "100M");
            config.put("neostore.nodestore.db.mapped_memory", "90M");
            config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");

            inserter = BatchInserters.inserter(DB_PATH, config);
            initialized = true;

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println("Shutting down batch inserter...");
                    long t = System.currentTimeMillis();
                    inserter.shutdown();
                    System.out.println("Done...: took " + (System.currentTimeMillis() - t) + " ms");
                }
            });


            SimpleDateFormat sdf = new java.text.SimpleDateFormat("YYYYMMDD_HHmmss");
            String logfileName = InetAddress.getLocalHost().getHostName().substring(0,8)  + "_NEO4J_" + sdf.format(new Date())  +"_" + currentPhase.toString() + "_P" +
                    ( (currentPhase.ordinal() == Phase.LOAD.ordinal()) ? p1.get("loaders") :  p1.get("requesters")  ) +"_V" + p1.get("maxid1") + ".log";
            System.out.println("Going to log to: " + logfileName);
            System.setOut(new PrintStream(new FileOutputStream(logfileName + ".out")));
            System.setErr(new PrintStream(new FileOutputStream(logfileName + ".err")));

            Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER).removeAllAppenders();
            EnhancedPatternLayout fmt = new EnhancedPatternLayout("%p %d [%t]: %m%n%throwable{30}");
            FileAppender console = new FileAppender(fmt, logfileName + ".log");
            Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER).addAppender(console);



        }
    }

    @Override
    public void close() {

    }

    @Override
    public void clearErrors(int threadID) {
        System.exit(0);
    }

    @Override
    public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
        if (currentPhase == Phase.LOAD) {
            try {
                inserter.createRelationship(a.id1, a.id2, (a.link_type % 2 == 0 ? RelTypes.TYPE0 : RelTypes.TYPE1), null);
            } catch (NotFoundException nfe) {
                System.err.println("Cannot find : " + nfe.getMessage() + ", idseq=" + idSequence.get());
                throw nfe;
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        return false;
    }

    @Override
    public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
        return false;
    }

    @Override
    public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        return null;
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        return new Link[0];
    }

    @Override
    public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        return new Link[0];
    }

    @Override
    public long countLinks(String dbid, long id1, long link_type) throws Exception {
        return 0;
    }

    @Override
    public void resetNodeStore(String dbid, long startID) throws Exception {

    }

    protected Map<String, Object> nodeToNeoNode(Node node) {
        HashMap<String, Object> props = new HashMap<>(10);
        props.put(ID, node.id);
        props.put(TIME, node.time);
        props.put(TYPE, node.type);
        props.put(VERSION, node.version);
        props.put(DATA, node.data);
        return props;
    }

    @Override
    public long addNode(String dbid, Node node) throws Exception {
        if (currentPhase == Phase.LOAD) {
            node.id = idSequence.getAndIncrement();
            inserter.createNode(node.id, null) ; // nodeToNeoNode(node));
            return node.id;
        }
        throw new NotImplementedException();
    }

    @Override
    public Node getNode(String dbid, int type, long id) throws Exception {
        return null;
    }

    @Override
    public boolean updateNode(String dbid, Node node) throws Exception {
        return false;
    }

    @Override
    public boolean deleteNode(String dbid, int type, long id) throws Exception {
        return false;
    }
}
