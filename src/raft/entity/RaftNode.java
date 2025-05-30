package raft.entity;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;



public class RaftNode {
     public enum Role {
        LEADER,
        FOLLOWER,
        CANDIDATE
    }

    // ====== Node Identity ======
    private final Alamat nodeId; // bisa IP:PORT atau ID unik
    private final List<Alamat> clusterNodes;

    // ====== Raft Core State ======
    private int currentTerm ;
    private Alamat votedFor ;

    private  List<Entry> log ;
    private int commitIndex ;
    private int lastApplied ;

    private Role role ;
    private Alamat currentLeader ;

    // ====== Timing ======
    private long lastHeartbeatTime;
    private long electionTimeout ;
    private static final long HEARTBEAT_INTERVAL = 150; // ms

    // ====== Leader State (Volatile) ======
    private Map<String, Integer> nextIndex = new ConcurrentHashMap<>();
    private Map<String, Integer> matchIndex = new ConcurrentHashMap<>();

    // ====== Application Layer ======
    private  KVStore kvStore ;

    // ====== Constructor ======
    public RaftNode() {
        this.nodeId = new Alamat();
        this.clusterNodes  = new ArrayList<>();
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = new ArrayList<>();
        this.commitIndex = -1 ;
        this.lastApplied = -1 ;
        this.role = Role.FOLLOWER;
        this.currentLeader = null ; 
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.electionTimeout = randomElectionTimeout();
        this.kvStore = new KVStore();
        
        // Bisa isi dummy log kalau mau
    }

    public RaftNode(Alamat nodeId, List<Alamat> clusterNodes, int currentTerm, Role role, Alamat votedFor, List<Entry> initialLog, int commitIdx, int  lastApplied, Alamat currentLeader) {
        this.nodeId = nodeId;
        this.clusterNodes = clusterNodes;
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = (initialLog != null) ? initialLog : new ArrayList<>();
        this.commitIndex = commitIdx;
        this.lastApplied = lastApplied;
        this.role = role;
        this.currentLeader = currentLeader;

        this.lastHeartbeatTime = System.currentTimeMillis();
        this.electionTimeout = randomElectionTimeout();
        this.kvStore = new KVStore();

        // Jika node ini adalah LEADER, siapkan nextIndex dan matchIndex
        if (role == Role.LEADER) {
            int lastLogIdx = log.size()-1; // Bisa juga log.size() - 1 tergantung implementasi
            for (Alamat peer : clusterNodes) {
                String peerId = peer.toString(); // Asumsi Alamat punya getId()
                if (!peer.equals(this.nodeId)) {
                    nextIndex.put(peerId, lastLogIdx);
                    matchIndex.put(peerId, -1);
                }
            }
        }
    }


    // ====== Utility: Election Timeout ======
    private long randomElectionTimeout() {
        return 150 + new Random().nextInt(150); // antara 150–300 ms
    }

    // ====== Timer Update ======
    public void resetHeartbeatTimer() {
        lastHeartbeatTime = System.currentTimeMillis();
    }

    public boolean isElectionTimeout() {
        return System.currentTimeMillis() - lastHeartbeatTime > electionTimeout;
    }

    // ====== State Getters ======
    public int getCurrentTerm() {
        return currentTerm;
    }

    public Alamat getVotedFor() {
        return votedFor;
    }

    public Role getRole() {
        return role;
    }

    public Alamat getNodeAdr() {
        return nodeId;
    }

    public Alamat getCurrentLeader() {
        return currentLeader;
    }

    public KVStore getKVStore() {
        return kvStore;
    }

    // TODO: Implementasi utama:
    // - requestVote
    // - appendEntries
    // - becomeLeader / becomeCandidate / becomeFollower
    // - commit log
    // - apply log entries to kvStore
}
