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
        if (role == Role.LEADER){
            initializeLeaderState();
        }

        resetHeartbeatTimer();
    }

    private void initializeLeaderState(){
        int lastLogIdx = log.size()-1; // Bisa juga log.size() - 1 tergantung implementasi
            for (Alamat peer : clusterNodes) {
                String peerId = peer.toString(); // Asumsi Alamat punya getId()
                if (!peer.equals(this.nodeId)) {
                    nextIndex.put(peerId, lastLogIdx);
                    matchIndex.put(peerId, -1);
                }
            }
    }

     private void startRaftLifecycle() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (!running) { // Check if node is stopped
                    scheduler.shutdown();
                    return;
                }
                step(); // Perform a step based on the current role
            } catch (Exception e) {
                System.err.println("[" + nodeId + "] Error in Raft lifecycle: " + e.getMessage());
                e.printStackTrace();
            }
        }, 0, 50, TimeUnit.MILLISECONDS); 
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
                System.out.println("[" + nodeId + "] Election timeout. Becoming CANDIDATE. Term: " + currentTerm);
                becomeCandidate();
            }
        }
        else if(role == Role.CANDIDATE){
            if(isElectionTimeout()){
                System.out.println("[" + nodeId + "] Election timeout again. Starting new election. Term: " + currentTerm);
                startElection();
            }
        }
        else if(role == Role.LEADER){
            if(System.currentTimeMillis() - lastHeartbeatTime >= HEARTBEAT_INTERVAL){
                System.out.println("[" + nodeId + "] Sending heartbeats. Term: " + currentTerm);
                sendHeartbeat();
                resetHeartbeatTimer();
            }
        }
    }

    // Update Role

     private void becomeFollower(int newTerm, Alamat newLeader) {
        System.out.println("[" + nodeId + "] Becoming FOLLOWER. Term: " + newTerm + ", Leader: " + newLeader);
        this.role = Role.FOLLOWER;
        this.currentTerm = newTerm;
        this.currentLeader = newLeader;
        this.votedFor = null; 
        resetHeartbeatTimer();
        votesReceived.set(0);
    }

    private void becomeCandidate() {
        System.out.println("[" + nodeId + "] Becoming CANDIDATE. Term: " + (currentTerm + 1));
        this.role = Role.CANDIDATE;
        this.currentTerm++; 
        this.votedFor = nodeId; 
        this.votesReceived.set(1);
        this.currentLeader = null; 
        resetHeartbeatTimer(); 
        startElection(); 
    }

    private void becomeLeader() {
        System.out.println("[" + nodeId + "] Becoming LEADER. Term: " + currentTerm);
        this.role = Role.LEADER;
        this.currentLeader = nodeId; 
        initializeLeaderState(); 
        sendHeartbeat(); 
        resetHeartbeatTimer(); 
        votesReceived.set(0); 
    }

    // Election

    private void startElection(){
        int peerCount = clusterNodes.size() - 1; 
        if (peerCount == 0) { 
            System.out.println("[" + nodeId + "] Single node cluster. Becoming LEADER immediately.");
            becomeLeader();
            return;
        }

        System.out.println("[" + nodeId + "] Starting election for Term " + currentTerm + ". Sending RequestVote RPCs.");
        votesReceived.set(1); 

        // Get last log entry details
        int lastLogIndex = log.isEmpty() ? -1 : log.size() - 1;
        int lastLogTerm = log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();

        // Use a thread pool for concurrent RPC calls
        Executors.newCachedThreadPool().submit(() -> {
            for (Alamat peer : clusterNodes) {
                if (!peer.equals(nodeId)) {
                    // Create RequestVote RPC client for this peer
                    RPCClient rpcClient = new RPCClient(peer); 

                    VoteReq voteRequest = new VoteReq(currentTerm, nodeId, peer, lastLogIndex, lastLogTerm);

                    try {
                        System.out.println("[" + nodeId + "] Requesting vote from " + peer + " for Term " + currentTerm);
                        VoteResp response = rpcClient.requestVote(voteRequest);

                        synchronized (electionLock) { // Synchronize access to shared state (votesReceived, role)
                            if (role != Role.CANDIDATE) {
                                // Already transitioned
                                System.out.println("[" + nodeId + "] No longer a candidate, ignoring vote response from " + peer);
                                return;
                            }

                            if (response.getTerm() > currentTerm) {
                                // Discover higher term, revert to follower 
                                System.out.println("[" + nodeId + "] Discovered higher term " + response.getTerm() + " from " + peer + ". Becoming FOLLOWER.");
                                becomeFollower(response.getTerm(), null); 
                                return;
                            }

                            if (response.isVoteGranted() && response.getTerm() == currentTerm) {
                                int currentVotes = votesReceived.incrementAndGet();
                                System.out.println("[" + nodeId + "] Received vote from " + peer + ". Total votes: " + currentVotes);
                                if (currentVotes > (clusterNodes.size() / 2)) { 
                                    System.out.println("[" + nodeId + "] Achieved majority votes. Becoming LEADER.");
                                    becomeLeader();
                                    return; // Stop
                                }
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("[" + nodeId + "] Failed to request vote from " + peer + ": " + e.getMessage());
                       
                    }
                }
            }
        });
    }

// ====== RPC Handlers (to be exposed by RPCServer) ======

    /**
     * RPC handler for RequestVote RPC.
     * Arguments: candidate's term, candidate requesting vote, index of candidate's last log entry, term of candidate's last log entry. 
     * Results: currentTerm, true means candidate received vote. 
     */
    public VoteResp requestVote(VoteReq request) {
        synchronized (electionLock) { // Protect shared state during vote processing
            System.out.println("[" + nodeId + "] Received RequestVote from " + request.getFrom() + " for Term " + request.getTerm());

            // 1. Reply false if term < currentTerm 
            if (request.getTerm() < currentTerm) {
                System.out.println("[" + nodeId + "] Denying vote: Request term " + request.getTerm() + " < current term " + currentTerm);
                return new VoteResp(currentTerm, false, nodeId, request.getFrom());
            }

            // If RPC request or response contains term T > currentTerm: set currentTerm = T. convert to follower 
            if (request.getTerm() > currentTerm) {
                System.out.println("[" + nodeId + "] Discovered higher term " + request.getTerm() + " from " + request.getFrom() + ". Becoming FOLLOWER.");
                becomeFollower(request.getTerm(), null); // Leader unknown yet
            }

            // 2. If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
            boolean canGrantVote = (votedFor == null || votedFor.equals(request.getCandidateId()));

            // Determine if candidate's log is at least as up-to-date as receiver's log 
            boolean isLogUpToDate = isCandidateLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm());

            if (canGrantVote && isLogUpToDate) {
                votedFor = request.getCandidateId(); // Grant vote
                System.out.println("[" + nodeId + "] Granting vote to " + request.getCandidateId() + " for Term " + currentTerm);
                resetHeartbeatTimer(); // Granting a vote also resets election timer
                return new VoteResp(currentTerm, true, nodeId, request.getFrom());
            } else {
                System.out.println("[" + nodeId + "] Denying vote for " + request.getCandidateId() + ". Reason: votedFor=" + votedFor + ", logUpToDate=" + isLogUpToDate);
                return new VoteResp(currentTerm, false, nodeId, request.getFrom());
            }
        }
    }

    /**
     * Helper to check if a candidate's log is at least as up-to-date as this node's log. 
     */
    private boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int ownLastLogIndex = log.isEmpty() ? -1 : log.size() - 1;
        int ownLastLogTerm = log.isEmpty() ? 0 : log.get(ownLastLogIndex).getTerm();

        // Raft rules: A candidate is up-to-date if:
        // 1. Its last log term is greater than the receiver's last log term, OR
        // 2. Its last log term is equal to the receiver's last log term, AND its last log index is greater than or equal to the receiver's. 
        return (candidateLastLogTerm > ownLastLogTerm) ||
               (candidateLastLogTerm == ownLastLogTerm && candidateLastLogIndex >= ownLastLogIndex);
    }


    /**
     * RPC handler for AppendEntries RPC. Also used as heartbeat. 
     * Arguments: leader's term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit. 
     * Results: currentTerm, true if follower contained entry matching prevLogIndex and prevLogTerm. 
     */
    public NewEntryResp appendEntries(NewEntryReq request) {
        synchronized (electionLock) { // Protect shared state
            System.out.println("[" + nodeId + "] Received AppendEntries from " + request.getFrom() + " for Term " + request.getTerm() + " (Heartbeat: " + request.getEntries().isEmpty() + ")");

            // 1. Reply false if term < currentTerm 
            if (request.getTerm() < currentTerm) {
                System.out.println("[" + nodeId + "] Denying AppendEntries: Request term " + request.getTerm() + " < current term " + currentTerm);
                return new NewEntryResp(currentTerm, false, nodeId, request.getFrom());
            }

            // If RPC request or response contains term T > currentTerm: set currentTerm = T. convert to follower 
            // If AppendEntries RPC received from new leader: convert to follower 
            if (request.getTerm() > currentTerm || role == Role.CANDIDATE || role == Role.LEADER) {
                System.out.println("[" + nodeId + "] Received valid AppendEntries from new leader " + request.getFrom() + ". Becoming FOLLOWER. Term: " + request.getTerm());
                becomeFollower(request.getTerm(), request.getFrom());
            } else { // Current role is Follower, term is equal
                // Valid heartbeat from current leader
                resetHeartbeatTimer(); // Reset timer for current leader                this.currentLeader = request.getFrom(); // Confirm current leader
            }

            // For heartbeats (empty entries), just acknowledge. Log replication logic will go here later.
            if (request.getEntries().isEmpty()) {
                // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) 
                // For heartbeats, last new entry is last existing entry if no new entries.
                if (request.getLeaderCommit() > commitIndex) {
                    commitIndex = Math.min(request.getLeaderCommit(), log.isEmpty() ? -1 : log.size() - 1);
                    // TODO: Apply committed entries to state machine if commitIndex > lastApplied
                }
                return new NewEntryResp(currentTerm, true, nodeId, request.getFrom());
            }

            // TODO: Implement log consistency check and append/truncate for non-heartbeat AppendEntries
            // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm 
            // 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it 
            // 4. Append any new entries not already in the log 
            // 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) 
            return new NewEntryResp(currentTerm, true, nodeId, request.getFrom()); // Placeholder
        }
    }

    private void sendHeartbeat() {
        // Send initial empty AppendEntries RPCs (heartbeat) to each server 
        // This method will be asynchronous.
        Executors.newCachedThreadPool().submit(() -> {
            for (Alamat peer : clusterNodes) {
                if (!peer.equals(nodeId)) {
                    RPCClient rpcClient = new RPCClient(peer); // Assuming RPCClient is constructed with target Alamat
                    NewEntryReq heartbeat = new NewEntryReq(
                        currentTerm, nodeId, // Leader ID
                        log.isEmpty() ? -1 : log.size() - 1, // prevLogIndex for consistency check
                        log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm(), // prevLogTerm
                        new ArrayList<>(), // Empty entries list for heartbeat
                        commitIndex, // Leader's commit index
                        nodeId, // From (leader)
                        peer // To (follower)
                    );

                    try {
                        // System.out.println("[" + nodeId + "] Sending heartbeat to " + peer);
                        NewEntryResp response = rpcClient.appendEntries(heartbeat);

                        synchronized (electionLock) { // Synchronize access to shared state (role)
                            if (role != Role.LEADER) {
                                // No longer leader, stop sending heartbeats from this thread
                                System.out.println("[" + nodeId + "] No longer leader, stopping heartbeat to " + peer);
                                return;
                            }

                            if (response.getTerm() > currentTerm) {
                                // Discover higher term, revert to follower 
                                System.out.println("[" + nodeId + "] Discovered higher term " + response.getTerm() + " from " + peer + ". Becoming FOLLOWER.");
                                becomeFollower(response.getTerm(), null); // Leader unknown yet
                                return;
                            }
                            // Handle response.isSuccess() for log replication (later)
                        }
                    } catch (Exception e) {
                        System.err.println("[" + nodeId + "] Failed to send heartbeat/appendEntries to " + peer + ": " + e.getMessage());
                        // Consider how to handle unresponsive followers (e.g., retry logic, decrement nextIndex for them)
                    }
                }
            }
        });
    }


    // ====== Node Shutdown ======
    public void shutdown() {
        running = false;
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        System.out.println("[" + nodeId + "] RaftNode shutdown.");
    }



}

