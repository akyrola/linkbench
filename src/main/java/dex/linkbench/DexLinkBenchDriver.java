package dex.linkbench;

import com.facebook.LinkBench.GraphStore;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;
import com.sparsity.dex.gdb.*;
import com.sparsity.dex.gdb.Objects;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 *  Because of license reasons, singleton and one session only!
 */
class DexLinkBenchDriver extends GraphStore {
    static DexLinkBenchDriverSingleton singleton = new DexLinkBenchDriverSingleton();

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

/**
 * @author Aapo Kyrola
 */


class DexLinkBenchDriverSingleton extends GraphStore {

    static Dex dex;
    static DexConfig conf;
    String dbfilename;
    String dbname = "linkbench8";
    static Database db;
    Session dex_session = null;
    Graph dex_graph = null;

    public DexLinkBenchDriverSingleton() {
        conf = new DexConfig();
    }

    private int edgeType(long fbtype) {
        return (int) (fbtype % 2);
    }
    Phase currentPhase;

    private static boolean initialized = false;

    @Override
    public synchronized void initialize(Properties p, Phase currentPhase, int threadId) throws IOException, Exception {
        synchronized (DexLinkBenchDriver.class){
            this.currentPhase = currentPhase;
            if (dex == null) dex = new Dex(conf);

            if (!initialized) {
                try {
                    if (currentPhase == Phase.LOAD) {

                        dbfilename = dbname + ".dex";
                        db = dex.create(dbfilename, dbname);
                        this.defineSchema();
                        this.closeDB();
                        System.err.println(new File(dbfilename).getAbsolutePath());

                        System.err.println("Successfully created database " + Thread.currentThread());

                    } else {
                    }

                    initialized = true;
                } catch (Exception e) {
                    System.err.println("Error: " + Thread.currentThread());
                    e.printStackTrace();
                }

            }
            if (db == null) openDB(dbname);

        }

        while(!initialized) {
            Thread.sleep(10);
        }

        this.getSchema();
        System.err.println("dexgraph now:" + dex_graph);

        if (dex_session == null || dex_session.isClosed()) {
            openTransaction();
        }

    }


    public synchronized  boolean openDB(String dbname) {
        dex = new Dex(conf);
        try {
            System.out.println("OpenDB");
            dbfilename = dbname + ".dex";
            db = dex.open(dbfilename, false);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
        return true;
    }

    public  synchronized   boolean closeDB() {
        try {
            System.out.println("CloseDB");
            db.close();
            db.delete();
            dex.delete();
            dex = null;
            db = null;
            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    public  synchronized  boolean openTransaction() {
        try {
            System.out.println("openTransaction");
            dex_session = db.newSession();
            dex_session.begin();
            dex_graph = dex_session.getGraph();
            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
    }


    public synchronized   boolean closeTransaction() {
        try {
            System.out.println("closeTransaction");

            dex_session.commit();
            dex_session.close();
            dex_session.delete();
            dex_session = null;
            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    int vertexType;
    int vid;
    int col_version;
    int col_timestamp;
    int col_vertex_payload;

    int edgeType0;
    int edgeType1;


    int ecol_version0;
    int ecol_timestamp0;
    int ecol_edge_payload0;
    int ecol_version1;
    int ecol_timestamp1;
    int ecol_edge_payload1;


    private  synchronized  void defineSchema() {
        if (db == null) {
            return;
        }
        if (this.openTransaction()) {
            //Person node type
            vertexType = dex_graph.newNodeType("v");
            vid = dex_graph.newAttribute(vertexType, "id", DataType.Long, AttributeKind.Unique);

            col_version = dex_graph.newAttribute(vertexType, "version", DataType.Integer, AttributeKind.Basic);
            col_timestamp = dex_graph.newAttribute(vertexType, "timestamp", DataType.Integer, AttributeKind.Basic); // Or indexed?
            col_vertex_payload = dex_graph.newAttribute(vertexType, "payload", DataType.String, AttributeKind.Basic);

            edgeType0 = dex_graph.newEdgeType("e0", true, false);     // directed=true, neighbors=false
            edgeType1 = dex_graph.newEdgeType("e1", true, false);

            ecol_version0 = dex_graph.newAttribute(edgeType0, "version_e0", DataType.Integer, AttributeKind.Basic);
            ecol_timestamp0 = dex_graph.newAttribute(edgeType0, "timestamp_e0", DataType.Long, AttributeKind.Basic);
            ecol_edge_payload0 = dex_graph.newAttribute(edgeType0, "payload_e0", DataType.String, AttributeKind.Basic);
            ecol_version1 = dex_graph.newAttribute(edgeType1, "version_e1", DataType.Integer, AttributeKind.Basic);
            ecol_timestamp1 = dex_graph.newAttribute(edgeType1, "timestamp_e1", DataType.Long, AttributeKind.Basic);
            ecol_edge_payload1 = dex_graph.newAttribute(edgeType1, "payload_e1", DataType.String, AttributeKind.Basic);

            this.closeTransaction();
        }
    }

    private  synchronized  void getSchema() {
        if (db == null) {
            return;
        }
        if (this.openTransaction()) {
            //Person node type
            vertexType = dex_graph.findType("v");
            vid = dex_graph.findAttribute(vertexType, "id");

            col_version = dex_graph.findAttribute(vertexType, "version");
            col_timestamp = dex_graph.findAttribute(vertexType, "timestamp");
            col_vertex_payload = dex_graph.findAttribute(vertexType, "payload");

            edgeType0 = dex_graph.findType("e0");   // directed=true, neighbors=true
            edgeType1 = dex_graph.findType("e1");

            ecol_version0 = dex_graph.findAttribute(edgeType0, "version_e0");
            ecol_timestamp0 = dex_graph.findAttribute(edgeType0, "timestamp_e0");
            ecol_edge_payload0 = dex_graph.findAttribute(edgeType0, "payload_e0");
            ecol_version1 = dex_graph.findAttribute(edgeType1, "version_e1");
            ecol_timestamp1 = dex_graph.findAttribute(edgeType1, "timestamp_e1");
            ecol_edge_payload1 = dex_graph.findAttribute(edgeType1, "payload_e1");
            this.closeTransaction();
        }
    }

    @Override
    public  synchronized  void close() {
        this.closeTransaction();
        this.closeDB();
    }

    @Override
    public  synchronized  void clearErrors(int threadID) {

    }

    @Override
    public  synchronized  boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
        Value dexvalue = new Value();

        if (a.id1 == a.id2) return false;

        long nd1 = dex_graph.findObject(vid, dexvalue.setLong(a.id1));
        long nd2 = dex_graph.findObject(vid, dexvalue.setLong(a.id2));

        if (nd1 != Objects.InvalidOID && nd2 != Objects.InvalidOID) {
            int etype = edgeType(a.link_type);
            int edgeType = etype == 0 ? edgeType0 : edgeType1;
            long e = (currentPhase == Phase.LOAD ? Objects.InvalidOID : getLinkId(a.id1, a.link_type, a.id2));
            if (e == Objects.InvalidOID) // Create only if did not exist
                e = dex_graph.newEdge(edgeType, nd1, nd2);
            if (e != Objects.InvalidOID) {
                dex_graph.setAttribute(e, etype == 0 ? ecol_timestamp0 : ecol_timestamp1, dexvalue.setLong(a.time));
                dex_graph.setAttribute(e, etype == 0 ? ecol_version0 : ecol_version1, dexvalue.setInteger(a.version));
                dex_graph.setAttribute(e, etype == 0 ? ecol_edge_payload0 : ecol_edge_payload1, dexvalue.setString(new String(a.data)));

                return true;
            } else {
                System.out.println("Could not add link: " + nd1 + "; " + nd2 + "; e="+e);
                return false;
            }
        } else {
            System.out.println("Could not add link2: " + nd1 + "; " + nd2);

            return false;
        }
    }

    public  synchronized   long getLinkId(long id1, long link_type, long id2) {
        Value dexvalue = new Value();

        long nd1 = dex_graph.findObject(vid, dexvalue.setLong(id1));
        long nd2 = dex_graph.findObject(vid, dexvalue.setLong(id2));
        if (nd1 == Objects.InvalidOID || nd2 == Objects.InvalidOID) {
            return Objects.InvalidOID;
        }
        return dex_graph.findEdge((edgeType(link_type) % 2 == 0 ? edgeType0 : edgeType1), nd1, nd2);
    }

    @Override
    public  synchronized   boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge) throws Exception {
        long e = getLinkId(id1, link_type, id2);
        if (e != Objects.InvalidOID) {
            dex_graph.drop(e);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public  synchronized  boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
        Value dexvalue = new Value();

        long nd1 = dex_graph.findObject(vid, dexvalue.setLong(a.id1));
        long nd2 = dex_graph.findObject(vid, dexvalue.setLong(a.id2));

        if (nd1 != Objects.InvalidOID && nd2 != Objects.InvalidOID) {
            long e = getLinkId(a.id1, a.link_type, a.id2);
            int etype = edgeType(a.link_type);
            if (e != Objects.InvalidOID) {
                dex_graph.setAttribute(e, etype == 0 ? ecol_timestamp0 : ecol_timestamp1, dexvalue.setLong(a.time));
                dex_graph.setAttribute(e, etype == 0 ? ecol_version0 : ecol_version1, dexvalue.setInteger(a.version));
                dex_graph.setAttribute(e, etype == 0 ? ecol_edge_payload0 : ecol_edge_payload1, dexvalue.setString(new String(a.data)));

                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public  synchronized  Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
        long e = getLinkId(id1, link_type, id2);
        if (e != Objects.InvalidOID) {
            int etype = edgeType(link_type);

            int version = dex_graph.getAttribute(e, etype == 0 ? ecol_version0 : ecol_version1).getInteger();
            long timestamp = dex_graph.getAttribute(e, etype == 0 ? ecol_timestamp0 : ecol_timestamp1).getLong();
            String payload =  dex_graph.getAttribute(e, etype == 0 ? ecol_edge_payload0 : ecol_edge_payload1).getString();
            return new Link(id1, link_type, id2, VISIBILITY_DEFAULT, payload.getBytes(), version, timestamp);
        } else {
            return null;
        }
    }

    class LinkTimestampComparatorDesc implements Comparator<Link> {
        @Override
        public int compare(Link o1, Link o2) {
            if (o1.time > o2.time) {
                return -1;
            } else {
                if (o1.time == o2.time) return 0;
                return 1;
            }
        }
    }

    private QuickSelect<Link> quickSelect = new QuickSelect<>(new LinkTimestampComparatorDesc());

    @Override
    public  synchronized  Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
        Value dexvalue = new Value();
        long etype = edgeType(link_type);
        long nd1 = dex_graph.findObject(vid, dexvalue.setLong(id1));
        if (nd1 != Objects.InvalidOID) {
            Objects linksObjs = dex_graph.explode(nd1, etype == 0 ? edgeType0 : edgeType1, EdgesDirection.Outgoing);
            ObjectsIterator iter = linksObjs.iterator();
            ArrayList<Link> links = new ArrayList<>();
            while(iter.hasNext()) {
                long e = iter.next();
                int version = dex_graph.getAttribute(e, etype == 0 ? ecol_version0 : ecol_version1).getInteger();
                long timestamp = dex_graph.getAttribute(e, etype == 0 ? ecol_timestamp0 : ecol_timestamp1).getLong();
                String payload =  dex_graph.getAttribute(e, etype == 0 ? ecol_edge_payload0 : ecol_edge_payload1).getString();
                long head = 0; // TODO
                links.add(new Link(id1, link_type, head, VISIBILITY_DEFAULT, payload.getBytes(), version, timestamp));
            }
            int n = links.size();
            Link[] arr = new Link[n];
            for(int i=0; i<n; i++) arr[i] = links.get(i);

            if (n > 10000) {
                Link pivot = quickSelect.select(arr, 10000);
                Link[] all = arr;
                arr = new Link[10000];

                int j = 0;
                for(int i=0; i<all.length; i++) {
                  if (j >= 10000) break;
                  if (all[i].time > pivot.time) arr[j++] = all[i];
                }
            }
            Arrays.sort(arr, new LinkTimestampComparatorDesc());

            if (arr.length > 1000) System.out.println("get link list(1):" + arr.length);
            iter.close();
            linksObjs.close();
            return arr;
        } else {
            System.out.println("Getlinklist failed: " + id1 + ", degree:" + countLinks(dbid, id1, link_type));
        }
        return new Link[0];
    }

    @Override
    public synchronized   Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset, int limit) throws Exception {
        Value dexvalue = new Value();
        long etype = edgeType(link_type);
        long nd1 = dex_graph.findObject(vid, dexvalue.setLong(id1));
        if (nd1 != Objects.InvalidOID) {
            Objects linksObjs = dex_graph.explode(nd1, etype == 0 ? edgeType0 : edgeType1, EdgesDirection.Outgoing);
            ObjectsIterator iter = linksObjs.iterator();
            ArrayList<Link> links = new ArrayList<>();
            while(iter.hasNext()) {
                long e = iter.next();
                long timestamp = dex_graph.getAttribute(e, etype == 0 ? ecol_timestamp0 : ecol_timestamp1).getLong();

                if (timestamp >= minTimestamp && timestamp <= maxTimestamp) {
                    int version = dex_graph.getAttribute(e, etype == 0 ? ecol_version0 : ecol_version1).getInteger();

                    String payload =  dex_graph.getAttribute(e, etype == 0 ? ecol_edge_payload0 : ecol_edge_payload1).getString();
                    long head = 0; // TODO

                    links.add(new Link(id1, link_type, head, VISIBILITY_DEFAULT, payload.getBytes(), version, timestamp));
                }
            }
            iter.close();
            linksObjs.close();
            int n = links.size();
            Link[] arr = new Link[n];
            for(int i=0; i<n; i++) arr[i] = links.get(i);

            if (n > limit) {
                Link pivot = quickSelect.select(arr, limit);
                Link[] all = arr;
                arr = new Link[limit];
                int j = 0;
                for(int i=0; i<all.length; i++) {
                    if (j >= limit) break;
                    if (all[i].time > pivot.time) arr[j++] = all[i];
                }
            }
            Arrays.sort(arr, new LinkTimestampComparatorDesc());

            System.out.println("get link list(2):" + arr.length + "/" + n + " min:" + minTimestamp + " -- " + maxTimestamp);
            return arr;
        }
        return new Link[0];
    }

    @Override
    public synchronized   long countLinks(String dbid, long id1, long link_type) throws Exception {
        Value dexvalue = new Value();

        long nd1 = dex_graph.findObject(vid, dexvalue.setLong(id1));
        if (nd1 != Objects.InvalidOID) {
            return dex_graph.degree(nd1, edgeType(link_type) == 0 ? edgeType0 : edgeType1, EdgesDirection.Outgoing);
        } else {
            return 0;
        }
    }

    @Override
    public  synchronized  void resetNodeStore(String dbid, long startID) throws Exception {

    }

    @Override
    public  synchronized  long addNode(String dbid, Node node) throws Exception {
        if (dex_graph == null) {
            System.err.println("dex_graph null!");
            System.exit(1);
        }

        if (currentPhase == Phase.LOAD || getNode(dbid, node.type, node.id) == null) {
            long nd = dex_graph.newNode(vertexType);

            if (nd == Objects.InvalidOID) {
                throw new IllegalStateException("Could not add vertex: nodetype=" + vertexType);
            }

            Value dexvalue = new Value();

            dex_graph.setAttribute(nd, vid, dexvalue.setLong(node.id));
            dex_graph.setAttribute(nd, col_version,  dexvalue.setInteger((int)node.version));
            dex_graph.setAttribute(nd, col_timestamp, dexvalue.setInteger(node.time));
            dex_graph.setAttribute(nd, col_vertex_payload, dexvalue.setString(new String(node.data, 0, node.data.length > 2047 ? 2047 : node.data.length)));

            if(node.id % 100000 == 0) System.out.println(" Node: " + node.id + " / " + nd);
        } else {
            updateNode(dbid, node);
        }
        return node.id;
    }

    @Override
    public  synchronized  Node getNode(String dbid, int type, long id) throws Exception {
        Value dexvalue = new Value();
        long nd1 = dex_graph.findObject(vid, dexvalue.setLong(id));

        if (nd1 != Objects.InvalidOID) {
            int version = dex_graph.getAttribute(nd1, col_version).getInteger();
            int timestamp = dex_graph.getAttribute(nd1, col_timestamp).getInteger();
            String payload = dex_graph.getAttribute(nd1, col_vertex_payload).getString();

            return new Node(id, type, version, timestamp, payload.getBytes());
        } else {
            return null;
        }
    }

    @Override
    public  synchronized  boolean updateNode(String dbid, Node node) throws Exception {
        Value dexvalue = new Value();

        long nd = dex_graph.findObject(vid, dexvalue.setLong(node.id));
        if (nd != Objects.InvalidOID) {
            dex_graph.setAttribute(nd, vid, dexvalue.setLong(node.id));
            dex_graph.setAttribute(nd, col_version,  dexvalue.setInteger((int)node.version));
            dex_graph.setAttribute(nd, col_timestamp, dexvalue.setInteger(node.time));
            dex_graph.setAttribute(nd, col_vertex_payload, dexvalue.setString(new String(node.data, 0, node.data.length > 2047 ? 2047 : node.data.length)));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public  synchronized  boolean deleteNode(String dbid, int type, long id) throws Exception {
        Value dexvalue = new Value();
        long nd1 = dex_graph.findObject(vid, dexvalue.setLong(id));
        if (nd1 != Objects.InvalidOID) {
            dex_graph.drop(nd1);
            return true;
        } else {
            return false;
        }
    }
}



 class QuickSelect<E> {
     Comparator<E> comp;

    public QuickSelect(Comparator<E> comp) {
        this.comp = comp;
    }

    private  int partition(E[] arr, int left, int right, int pivot) {
        E pivotVal = arr[pivot];
        swap(arr, pivot, right);
        int storeIndex = left;
        for (int i = left; i < right; i++) {
            if (comp.compare(arr[i], pivotVal) < 0) {
                swap(arr, i, storeIndex);
                storeIndex++;
            }
        }
        swap(arr, right, storeIndex);
        return storeIndex;
    }

    public E select(E[] arr, int n) {
        int left = 0;
        int right = arr.length - 1;
        Random rand = new Random();
        while (right > left) {
            int pivotIndex = partition(arr, left, right, rand.nextInt(right - left + 1) + left);
            if (pivotIndex - left == n) {
                right = left = pivotIndex;
            } else if (pivotIndex - left < n) {
                n -= pivotIndex - left + 1;
                left = pivotIndex + 1;
            } else {
                right = pivotIndex - 1;
            }
        }
        return arr[left];
    }

    private static void swap(Object[] arr, int i1, int i2) {
        if (i1 != i2) {
            Object temp = arr[i1];
            arr[i1] = arr[i2];
            arr[i2] = temp;
        }
    }



}