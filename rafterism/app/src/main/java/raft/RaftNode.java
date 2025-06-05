package raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import raft.NodeAddr;
import raft.Entry;
import raft.KVStore;
import raft.NewVoteReq;
import raft.NewVoteResp;
import raft.NewEntryReq;
import raft.NewEntryResp;
import raft.RPCClient;  

public class RaftNode {
     public enum Role {
        LEADER,
        FOLLOWER,
        CANDIDATE
    }

    // ====== Node Identity ======
    private final NodeAddr nodeId; // bisa IP:PORT atau ID unik
    private final List<NodeAddr> clusterNodes;

    // ====== Raft Core State (Persistent) ======
    private int currentTerm ;
    private NodeAddr votedFor ;
    private  List<Entry> log ;

    private int commitIndex ;
    private int lastApplied ;

    private Role role ;
    private NodeAddr currentLeader ;

    // ====== Timing ======
    private long lastHeartbeatTime;
    private long electionTimeout ;
    private static final long HEARTBEAT_INTERVAL = 150; // ms
    

    // ====== Leader State (Volatile) ======
    private Map<NodeAddr, Integer> nextIndex = new ConcurrentHashMap<>();
    private Map<NodeAddr, Integer> matchIndex = new ConcurrentHashMap<>();

    // ====== Application Layer ======
    private  KVStore kvStore ;

    // ===== Election Specifics =====
    private AtomicInteger votesReceived;
    private final Object stateLock = new Object();

    // ==== Concurrency Util =====
    private volatile boolean running;
    private ScheduledExecutorService scheduler;

    // ====== Constructor ======
    public RaftNode(NodeAddr nodeId, List<NodeAddr> initialClusterNodes) {
        this.nodeId = nodeId;
        this.clusterNodes  = new ArrayList<>(initialClusterNodes);
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
        

        //Election
        this.votesReceived = new AtomicInteger(0);
        this.running = true;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        
        startRaftLifecycle();
    }

    public RaftNode(NodeAddr nodeId, List<NodeAddr> clusterNodes, int currentTerm, Role role, NodeAddr votedFor, List<Entry> initialLog, int commitIdx, int  lastApplied, NodeAddr currentLeader) {
        this(nodeId, clusterNodes);
        synchronized (stateLock) {
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
            this.log = (initialLog != null) ? new ArrayList<>(initialLog) : new ArrayList<>();
            this.commitIndex = commitIdx;
            this.lastApplied = lastApplied;
            this.role = role;
            this.currentLeader = currentLeader;

            if (role == Role.LEADER) {
                initializeLeaderState();
            }
        }
        resetHeartbeatTimer();
    }

    private void initializeLeaderState(){
        int lastLogIdx = log.size()-1; // Bisa juga log.size() - 1 tergantung implementasi
            for (NodeAddr peer : clusterNodes) {
                if (!peer.equals(this.nodeId)) {
                    nextIndex.put(peer, lastLogIdx);
                    matchIndex.put(peer, -1);
                }
            }
    }

      private void startRaftLifecycle() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (!running) {
                    scheduler.shutdown();
                    return;
                }
                step();
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

    public NodeAddr getVotedFor() {
        return votedFor;
    }

    public Role getRole() {
        return role;
    }

    public NodeAddr getNodeAdr() {
        return nodeId;
    }

    public NodeAddr getCurrentLeader() {
        return currentLeader;
    }

    public KVStore getKVStore() {
        return kvStore;
    }

    public List<Entry> getLogs() {
        return Collections.unmodifiableList(log); 
    }

    // TODO: Implementasi utama:
    // - requestVote
    // - appendEntries
    // - becomeLeader / becomeCandidate / becomeFollower
    // - commit log
    // - apply log entries to kvStore

    // Step

    private void step() {
        synchronized (stateLock) {
            applyCommittedEntries();

            if (role == Role.FOLLOWER) {
                if (isElectionTimeout()) {
                    System.out.println("[" + nodeId + "] Election timeout. Becoming CANDIDATE. Term: " + currentTerm);
                    becomeCandidate();
                }
            } else if (role == Role.CANDIDATE) {
                if (isElectionTimeout()) {
                    System.out.println("[" + nodeId + "] Election timeout again. Starting new election. Term: " + currentTerm);
                    startElection();
                }
            } else if (role == Role.LEADER) {
                if (System.currentTimeMillis() - lastHeartbeatTime >= HEARTBEAT_INTERVAL) {
                    // Send heartbeats and replicate logs
                    System.out.println("[" + nodeId + "] Sending heartbeats/replicating logs. Term: " + currentTerm);
                    sendAppendEntriesToFollowers();
                    resetHeartbeatTimer(); // Reset leader's own heartbeat timer
                }
                // Check for log commitment
                checkLogCommitment();
            }
        }
    }

    private void applyCommittedEntries() {
        synchronized (stateLock) {
            while (commitIndex > lastApplied) {
                lastApplied++;
                if (lastApplied >= 0 && lastApplied < log.size()) {
                    Entry entry = log.get(lastApplied);
                    System.out.println("[" + nodeId + "] Applying log entry[" + lastApplied + "]: " + entry);

                    // Correctly dispatch commands to KVStore based on entry.getCommand()
                    kvStore.executeCommand(entry.getCommand(), entry.getKey(), entry.getValue());

                } else {
                    System.err.println("[" + nodeId + "] Error: lastApplied (" + lastApplied + ") out of bounds for log size (" + log.size() + "). CommitIndex: " + commitIndex);
                    break;
                }
            }
        }
    }

    private void checkLogCommitment() {
        synchronized (stateLock) {
            int majority = (clusterNodes.size() / 2) + 1;
            // Iterate backwards from highest index down to commitIndex + 1
            // to find the highest N for which a majority of matchIndex[i] >= N
            // and log[N].term == currentTerm
            for (int N = log.size() - 1; N > commitIndex; N--) {
                int replicatedCount = 0;
                if (N < 0 || N >= log.size() || log.get(N).getTerm() != currentTerm) {
                    // Only commit entries from the current term
                    // N < 0 or N >= log.size() check for safety although loop condition N > commitIndex should handle N>=0
                    continue;
                }
                for (NodeAddr peer : clusterNodes) {
                    if (peer.equals(nodeId)) {
                        replicatedCount++; // Leader's own log is always replicated
                    } else {
                        if (matchIndex.containsKey(peer) && matchIndex.get(peer) >= N) {
                            replicatedCount++;
                        }
                    }
                }
                if (replicatedCount >= majority) {
                    commitIndex = N;
                    System.out.println("[" + nodeId + "] Committed index " + commitIndex);
                    applyCommittedEntries(); // Apply newly committed entries
                    break; // Found the highest N that can be committed
                }
            }
        }
    }

    // Update Role

     private void becomeFollower(int newTerm, NodeAddr newLeader) {
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
        sendAppendEntriesToFollowers(); 
        resetHeartbeatTimer(); 
        votesReceived.set(0); 
    }

    // Election

    private void startElection(){
        synchronized(stateLock){
        int peerCount = clusterNodes.size() - 1; 
        if (peerCount == 0) { 
            System.out.println("[" + nodeId + "] Single node cluster. Becoming LEADER immediately.");
            becomeLeader();
            return;
        }

        System.out.println("[" + nodeId + "] Starting election for Term " + currentTerm + ". Sending RequestVote RPCs.");
        
        // Get last log detail
        int lastLogIndex = log.isEmpty() ? -1 : log.size() - 1;
        int lastLogTerm = log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();

        // concurrent RPC calls
        Executors.newCachedThreadPool().submit(() -> {
                for (NodeAddr peer : clusterNodes) {
                    if (!peer.equals(nodeId)) {
                        final NodeAddr currentPeer = peer; // For use in lambda
                        // Use a new thread for each RPC to avoid blocking the main Raft lifecycle thread
                        // This thread should be managed better in a real setup (e.g., dedicated pool for RPCs)
                        new Thread(() -> {
                            RPCClient rpcClient = new RPCClient(currentPeer);
                            NewVoteReq NewVoteRequest = new NewVoteReq(currentTerm, nodeId, currentPeer, lastLogIndex, lastLogTerm);

                            try {
                                System.out.println("[" + nodeId + "] Requesting vote from " + currentPeer + " for Term " + currentTerm);
                                NewVoteResp response = rpcClient.requestVote(NewVoteRequest);

                                synchronized (stateLock) {
                                    // If term T > currentTerm: set currentTerm = T. convert to follower
                                    if (response.getTerm() > currentTerm) {
                                        System.out.println("[" + nodeId + "] Discovered higher term " + response.getTerm() + " from " + currentPeer + ". Becoming FOLLOWER.");
                                        becomeFollower(response.getTerm(), null);
                                        return;
                                    }

                                    // If role = CANDIDATE and term = currentTerm
                                    if (role == Role.CANDIDATE && response.getTerm() == currentTerm) {
                                        if (response.isVoteGranted()) {
                                            int currentVotes = votesReceived.incrementAndGet();
                                            System.out.println("[" + nodeId + "] Received vote from " + currentPeer + ". Total votes: " + currentVotes);
                                            if (currentVotes > (clusterNodes.size() / 2)) {
                                                System.out.println("[" + nodeId + "] Achieved majority votes. Becoming LEADER.");
                                                becomeLeader();
                                                return;
                                            }
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                System.err.println("[" + nodeId + "] Failed to request vote from " + currentPeer + ": " + e.getMessage());
                            }
                        }).start();
                    }
                }
            });
    }
    }

    public NewVoteResp requestVote(NewVoteReq request) {
        synchronized (stateLock) {
            System.out.println("[" + nodeId + "] Received RequestVote from " + request.getFrom() + " for Term " + request.getTerm());

            // Reply false if term < currentTerm
            if (request.getTerm() < currentTerm) {
                System.out.println("[" + nodeId + "] Denying vote: Request term " + request.getTerm() + " < current term " + currentTerm);
                return new NewVoteResp(currentTerm, false, nodeId, request.getFrom());
            }

            // If term T > currentTerm: set currentTerm = T. convert to follower
            if (request.getTerm() > currentTerm) {
                System.out.println("[" + nodeId + "] Discovered higher term " + request.getTerm() + " from " + request.getFrom() + ". Becoming FOLLOWER.");
                becomeFollower(request.getTerm(), null);
            }

            // If votedFor == null or candidateId, and candidate's log is at least as up-to-date, grant vote
            boolean canGrantVote = (votedFor == null || votedFor.equals(request.getCandidateId()));

            boolean isLogUpToDate = isCandidateLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm());

            if (canGrantVote && isLogUpToDate) {
                votedFor = request.getCandidateId(); // Grant vote
                System.out.println("[" + nodeId + "] Granting vote to " + request.getCandidateId() + " for Term " + currentTerm);
                resetHeartbeatTimer(); 
                return new NewVoteResp(currentTerm, true, nodeId, request.getFrom());
            } else {
                System.out.println("[" + nodeId + "] Denying vote for " + request.getCandidateId() + ". Reason: votedFor=" + votedFor + ", logUpToDate=" + isLogUpToDate);
                return new NewVoteResp(currentTerm, false, nodeId, request.getFrom());
            }
        }
    }

    private boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        synchronized (stateLock) {
            int ownLastLogIndex = log.isEmpty() ? -1 : log.size() - 1;
            int ownLastLogTerm = log.isEmpty() ? 0 : log.get(ownLastLogIndex == -1 ? 0 : ownLastLogIndex).getTerm(); // Handle empty log for term

            return (candidateLastLogTerm > ownLastLogTerm) ||
                   (candidateLastLogTerm == ownLastLogTerm && candidateLastLogIndex >= ownLastLogIndex);
        }
    }

    public NewEntryResp appendEntries(NewEntryReq request) {
        synchronized (stateLock) {
            System.out.println("[" + nodeId + "] Received AppendEntries from " + request.getFrom() + " for Term " + request.getTerm() + " (Heartbeat: " + request.getEntries().isEmpty() + ")");

            // Reply false if term < currentTerm
            if (request.getTerm() < currentTerm) {
                System.out.println("[" + nodeId + "] Denying AppendEntries: Request term " + request.getTerm() + " < current term " + currentTerm);
                return new NewEntryResp(currentTerm, false, nodeId, request.getFrom());
            }

            // If term T > currentTerm: set currentTerm = T. convert to follower
            // If AppendEntries RPC received from new leader: convert to follower
            if (request.getTerm() > currentTerm || role != Role.FOLLOWER) { 
                System.out.println("[" + nodeId + "] Received valid AppendEntries from new leader " + request.getFrom() + ". Becoming FOLLOWER. Term: " + request.getTerm());
                becomeFollower(request.getTerm(), request.getFrom());
            } else { // Current role is Follower, term is equal
                // Valid heartbeat/AppendEntries from current leader
                resetHeartbeatTimer();
                this.currentLeader = request.getFrom();
            }

            // --- Log Consistency Check ---
            // Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            // This is ONLY for non-heartbeat. Heartbeats don't need log consistency checks.
            if (request.getPrevLogIdx() >= 0) { 
                if (log.size() <= request.getPrevLogIdx() || log.get(request.getPrevLogIdx()).getTerm() != request.getPrevLogTerm()) {
                    System.out.println("[" + nodeId + "] Denying AppendEntries: Log consistency check failed.");
                    System.out.println("  PrevLogIdx: " + request.getPrevLogIdx() + ", PrevLogTerm: " + request.getPrevLogTerm());
                    System.out.println("  Own Log Size: " + log.size() + ", Own Term at Index: " + (log.size() > request.getPrevLogIdx() ? log.get(request.getPrevLogIdx()).getTerm() : "N/A"));
                    return new NewEntryResp(currentTerm, false, nodeId, request.getFrom());
                }
            }

            // If an existing entry conflicts with a new one (same index but different terms),
            // delete the existing entry and all that follow it.
            // Append any new entries not already in the log.
            for (int i = 0; i < request.getEntries().size(); i++) {
                Entry newEntry = request.getEntries().get(i);
                int logIndex = request.getPrevLogIdx() + 1 + i;

                if (logIndex < log.size()) { // If entry exists at this index
                    if (log.get(logIndex).getTerm() != newEntry.getTerm()) {
                        // Conflict
                        System.out.println("[" + nodeId + "] Conflicting entry at index " + logIndex + ". Truncating log.");
                        log = new ArrayList<>(log.subList(0, logIndex));
                        log.add(newEntry); // Append the new entry
                    } else {
                        // Same index, same term == duplicate.
                    }
                } else {
                    // Index is beyond current log size, simply append new entry
                    log.add(newEntry);
                    System.out.println("[" + nodeId + "] Appended new entry at index " + logIndex + ": " + newEntry);
                }
            }

            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (request.getLeaderCommit() > commitIndex) {
                // Last log index received from leader
                int lastReceivedLogIndex = request.getPrevLogIdx() + request.getEntries().size();
                commitIndex = Math.min(request.getLeaderCommit(), lastReceivedLogIndex);
                System.out.println("[" + nodeId + "] Updated commitIndex to " + commitIndex);
                applyCommittedEntries(); // Apply newly committed entries
            }

            return new NewEntryResp(currentTerm, true, nodeId, request.getFrom());}
        }

     private void sendAppendEntriesToFollowers() {
        synchronized (stateLock) {
            if (role != Role.LEADER) return; // Should not happen

            // For each follower, send AppendEntries RPC
            Executors.newCachedThreadPool().submit(() -> {
                for (NodeAddr peer : clusterNodes) {
                    if (!peer.equals(nodeId)) {
                        final NodeAddr currentPeer = peer;
                        // Use a new thread for each RPC to avoid blocking.
                        // For production, consider using a fixed thread pool or CompletableFuture for better management.
                        new Thread(() -> {
                            RPCClient rpcClient = new RPCClient(currentPeer);
                            int nextIdx = nextIndex.getOrDefault(currentPeer, log.size()); // Get nextIndex for this peer
                            int prevLogIdx = nextIdx - 1;
                            int prevLogTerm = (prevLogIdx >= 0 && prevLogIdx < log.size()) ? log.get(prevLogIdx).getTerm() : 0;

                            // Entries to send to this follower (from nextIdx onwards)
                            List<Entry> entriesToSend = new ArrayList<>();
                            if (nextIdx < log.size()) {
                                entriesToSend.addAll(log.subList(nextIdx, log.size()));
                            }

                            NewEntryReq req = new NewEntryReq(
                                currentTerm, nodeId, // Leader ID
                                prevLogIdx, prevLogTerm,
                                entriesToSend,
                                commitIndex, // Leader's commit index
                                nodeId, currentPeer // From and To
                            );

                            try {
                                NewEntryResp response = rpcClient.appendEntries(req);

                                synchronized (stateLock) {
                                    if (role != Role.LEADER) return; // No longer leader, stop

                                    if (response.getTerm() > currentTerm) {
                                        System.out.println("[" + nodeId + "] Discovered higher term " + response.getTerm() + " from " + currentPeer + ". Becoming FOLLOWER.");
                                        becomeFollower(response.getTerm(), null);
                                        return;
                                    }

                                    if (response.isSuccess()) {
                                        // Update nextIndex and matchIndex for this follower
                                        // nextIndex = index of the log entry immediately following the new ones.
                                        // matchIndex = highest log entry known to be replicated on the follower.
                                        int newMatchIndex = prevLogIdx + entriesToSend.size(); // The index of the last entry successfully replicated
                                        matchIndex.put(currentPeer, newMatchIndex);
                                        nextIndex.put(currentPeer, newMatchIndex + 1);
                                        System.out.println("[" + nodeId + "] Log replicated to " + currentPeer + ". matchIndex: " + newMatchIndex + ", nextIndex: " + (newMatchIndex + 1));
                                    } else {
                                        // AppendEntries failed due to log inconsistency (Rule 8)
                                        // Decrement nextIndex and retry (next heartbeat/AppendEntries cycle)
                                        // This will cause the leader to send a shorter set of logs on the next go.
                                        nextIndex.computeIfPresent(currentPeer, (k, v) -> Math.max(0, v - 1));
                                        System.out.println("[" + nodeId + "] AppendEntries to " + currentPeer + " failed. Decrementing nextIndex to " + nextIndex.get(currentPeer));
                                    }
                                }
                            } catch (Exception e) {
                                System.err.println("[" + nodeId + "] Failed to send AppendEntries to " + currentPeer + ": " + e.getMessage());
                            }
                        }).start();
                    }
                }
            });
        }
    }

    public String handleClientRequest(String commandType, String key, String value) {
        synchronized (stateLock) {
            if (role != Role.LEADER) {
                // Redirect client to leader
                return "REDIRECT:" + (currentLeader != null ? currentLeader.toString() : "UNKNOWN");
            }

            // Create an Entry for the command
            Entry newEntry = new Entry(currentTerm, commandType, key, value);
            log.add(newEntry); 
            System.out.println("[" + nodeId + "] Leader appended client command to log: " + newEntry);

            
            return "OK_PENDING_COMMIT"; 
        }
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