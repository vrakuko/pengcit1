package raft.entity;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import raft.msg.*;
import raft.server.*;


public class RaftNode {
     public enum Role {
        LEADER,
        FOLLOWER,
        CANDIDATE
    }

    // ====== Node Identity ======
    private final Alamat nodeId; // bisa IP:PORT atau ID unik
    private final List<Alamat> clusterNodes;

    // ====== Raft Core State (Persistent) ======
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

    // ===== Election Specifics =====
    private AtomicInteger votesReceived;
    private final Object electionLock = new Object();

    // ==== Concurrency Util =====
    private volatile boolean running;
    private ScheduledExecutorService scheduler;

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

        //Election
        this.votesReceived = new AtomicInteger(0);
        this.running = true;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();

        // Start Lifecycle for the first Node
        
        startRaftLifecycle();
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

    private void startRaftLifecycle(){
        
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

    // Step

    private void step(){
        if(role == Role.FOLLOWER){
            if(isElectionTimeout()){
                System.out.println("");
                becomeCandidate();
            }
        }
        else if(role == Role.CANDIDATE){
            if(isElectionTimeout()){
                
                startElection();
            }
        }
        else if(role == Role.LEADER){
            if(System.currentTimeMillis() - lastHeartbeatTime >= HEARTBEAT_INTERVAL){
                
                sendHeartbeat();
                resetHeartbeatTimer();
            }
        }
    }

    // Update Role

    private void becomeFollower(){

    }
    
    private void becomeCandidate(){

    }

    private void becomeLeader(){

    }

    // Election

    private void startElection(){

    }


    // 

    private void sendHeartbeat(){

    }

}

