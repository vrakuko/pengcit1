package raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import raft.proto.AppendEntriesRequest;
import raft.proto.AppendEntriesResponse;
import raft.proto.VoteRequest;
import raft.proto.VoteResponse;

public class RaftNode {
    public enum Role {
        LEADER,
        FOLLOWER,
        CANDIDATE
    }

    // ====== Node Identity ======
    private final NodeAddr nodeId;
    private final List<NodeAddr> clusterNodes;

    // ====== Raft Core State (Persistent) ======
    private volatile int currentTerm;
    private volatile NodeAddr votedFor;
    private final List<Entry> log;

    private volatile int commitIndex;
    private volatile int lastApplied;

    private volatile Role role;
    private volatile NodeAddr currentLeader;

    // ====== Timing ======
    private volatile long lastHeartbeatTime;
    private final long electionTimeout;
    private static final long HEARTBEAT_INTERVAL_MS = 1000;
    private static final long ELECTION_TIMEOUT_MIN_MS = 5000;
    private static final long ELECTION_TIMEOUT_MAX_MS = 8000;

    // ====== Leader State (Volatile) ======
    private final Map<NodeAddr, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<NodeAddr, Integer> matchIndex = new ConcurrentHashMap<>();

    // ====== Application Layer ======
    private final KVStore kvStore;
    private final Map<Integer, CompletableFuture<String>> pendingRequests = new ConcurrentHashMap<>();

    // ===== Election Specifics =====
    private final AtomicInteger votesReceived = new AtomicInteger(0);
    private final Object stateLock = new Object();

    // ==== Concurrency Util ====
    private volatile boolean running = true;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);

    // ====== Constructor ======
    public RaftNode(NodeAddr nodeId, List<NodeAddr> initialClusterNodes) {
        this.nodeId = nodeId;
        this.clusterNodes = new ArrayList<>(initialClusterNodes);
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = Collections.synchronizedList(new ArrayList<>()); // Gunakan list yang thread-safe
        this.commitIndex = -1;
        this.lastApplied = -1;
        this.role = Role.FOLLOWER;
        this.currentLeader = null;
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.electionTimeout = randomElectionTimeout();
        this.kvStore = new KVStore();

        startRaftLifecycle();
    }

    private void startRaftLifecycle() {
        scheduler.scheduleAtFixedRate(this::step, 0, 50, TimeUnit.MILLISECONDS);
        System.out.println("[" + nodeId + "] Raft node lifecycle started.");
    }

    // ====== Main State Machine ======
    private void step() {
        if (!running) return;
        applyCommittedEntries();

        switch (role) {
            case FOLLOWER:
            case CANDIDATE:
                if (isElectionTimeout()) {
                    System.out.println("[" + nodeId + "][Term " + currentTerm + "] Election timeout reached. Becoming candidate.");
                    becomeCandidate();
                }
                break;
            case LEADER:
                if (System.currentTimeMillis() - lastHeartbeatTime >= HEARTBEAT_INTERVAL_MS) {
                    // System.out.println("[" + nodeId + "][Term " + currentTerm + "] Sending heartbeat..."); //  log untuk debug
                    sendAppendEntriesToFollowers(true);
                    resetHeartbeatTimer();
                }
                break;
        }
    }
    
    private void becomeFollower(int newTerm, NodeAddr newLeader) {
        synchronized (stateLock) {
            System.out.println("[" + nodeId + "][Term " + currentTerm + "] Transitioning to FOLLOWER for term " + newTerm + ". Leader: " + newLeader);
            role = Role.FOLLOWER;
            currentTerm = newTerm;
            votedFor = null;
            currentLeader = newLeader;
            votesReceived.set(0);
            resetHeartbeatTimer();
        }
    }

    private void becomeCandidate() {
        synchronized (stateLock) {
            role = Role.CANDIDATE;
            currentTerm++;
            System.out.println("[" + nodeId + "][Term " + currentTerm + "] Transitioning to CANDIDATE.");
            votedFor = nodeId;
            currentLeader = null;
            votesReceived.set(1); // Vote for self
            resetHeartbeatTimer();
            startElection();
        }
    }

    private void becomeLeader() {
        synchronized (stateLock) {
            if (role != Role.CANDIDATE) return; // Hanya kandidat yang bisa jadi leader
            
            System.out.println("[" + nodeId + "][Term " + currentTerm + "] Transitioning to LEADER.");
            role = Role.LEADER;
            currentLeader = nodeId;
            
            // Re-inisialisasi state leader
            int lastLogIndex = log.size();
            for (NodeAddr peer : clusterNodes) {
                if (!peer.equals(nodeId)) {
                    nextIndex.put(peer, lastLogIndex);
                    matchIndex.put(peer, -1);
                }
            }
            sendAppendEntriesToFollowers(true);
        }
    }

    private void startElection() {
        final int electionTerm = this.currentTerm;
        System.out.println("[" + nodeId + "][Term " + electionTerm + "] Starting election.");
    
        int lastLogIndex = log.size() - 1;
        int lastLogTerm = lastLogIndex >= 0 ? log.get(lastLogIndex).getTerm() : 0;
    
        VoteRequest.Builder requestBuilder = VoteRequest.newBuilder()
            .setTerm(electionTerm)
            .setCandidateId(nodeId.toMsg())
            .setLastLogIndex(lastLogIndex)
            .setLastLogTerm(lastLogTerm);
    
        for (NodeAddr peer : clusterNodes) {
            if (!peer.equals(nodeId)) {
                VoteRequest request = requestBuilder.setToNodeId(peer.toMsg()).build();
                CompletableFuture.supplyAsync(() -> RPCWrapper.callRequestVote(peer, request), scheduler)
                    .thenAccept(response -> handleVoteResponse(response, electionTerm))
                    .exceptionally(ex -> {
                        System.err.println("[" + nodeId + "] Failed to send VoteRequest to " + peer + ": " + ex.getMessage());
                        return null;
                    });
            }
        }
    }

    private void handleVoteResponse(VoteResponse response, int electionTerm) {
        synchronized (stateLock) {
            if (role != Role.CANDIDATE || currentTerm != electionTerm) {
                return;
            }
            if (response.getTerm() > currentTerm) {
                becomeFollower(response.getTerm(), null);
                return;
            }
            if (response.getVoteGranted()) {
                int totalVotes = votesReceived.incrementAndGet();
                System.out.println("[" + nodeId + "][Term " + currentTerm + "] Vote granted by " + NodeAddr.fromMsg(response.getFromNodeId()) + ". Total votes: " + totalVotes);
                if (totalVotes > clusterNodes.size() / 2) {
                    becomeLeader();
                }
            }
        }
    }
    
    private void sendAppendEntriesToFollowers(boolean isHeartbeat) {
        if (role != Role.LEADER) return;
        for (NodeAddr peer : clusterNodes) {
            if (!peer.equals(nodeId)) {
                int nextIdx = nextIndex.getOrDefault(peer, log.size());

                if (isHeartbeat || log.size() >= nextIdx) {
                    int prevLogIndex = nextIdx - 1;
                    int prevLogTerm = (prevLogIndex >= 0) ? log.get(prevLogIndex).getTerm() : 0;

                    List<Entry> entriesToSend = (log.size() > nextIdx) ?
                        log.subList(nextIdx, log.size()) : Collections.emptyList();

                    if (isHeartbeat && entriesToSend.isEmpty()) {
                        // System.out.println("[" + nodeId + "][Term " + currentTerm + "] Sending heartbeat to " + peer);
                    } else if (!entriesToSend.isEmpty()) {
                        System.out.println("[" + nodeId + "][Term " + currentTerm + "] Replicating " + entriesToSend.size() + " entries to " + peer + " (from index " + nextIdx + ")");
                    }


                    AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                        .setTerm(currentTerm)
                        .setLeaderId(nodeId.toMsg())
                        .setPrevLogIndex(prevLogIndex)
                        .setPrevLogTerm(prevLogTerm)
                        .addAllEntries(entriesToSend.stream().map(Entry::toMsg).collect(Collectors.toList()))
                        .setLeaderCommit(commitIndex)
                        .setToNodeId(peer.toMsg())
                        .build();

                    CompletableFuture.supplyAsync(() -> RPCWrapper.callAppendEntries(peer, request), scheduler)
                        .thenAccept(response -> handleAppendEntriesResponse(peer, request, response))
                        .exceptionally(ex -> {
                            System.err.println("[" + nodeId + "][Term " + currentTerm + "] Failed to send AppendEntries to " + peer + ": " + ex.getMessage());
                            return null;
                        });
                }
            }
        }
        resetHeartbeatTimer();
    }

    private void handleAppendEntriesResponse(NodeAddr peer, AppendEntriesRequest request, AppendEntriesResponse response) {
        synchronized (stateLock) {
            if (role != Role.LEADER || currentTerm != request.getTerm()) return;

            if (response.getTerm() > currentTerm) {
                becomeFollower(response.getTerm(), null);
                return;
            }
            
            if (response.getSuccess()) {
                int newMatchIndex = request.getPrevLogIndex() + request.getEntriesCount();
                matchIndex.put(peer, newMatchIndex);
                nextIndex.put(peer, newMatchIndex + 1);
                updateCommitIndex();
            } else {
                int currentNextIndex = nextIndex.get(peer);
                nextIndex.put(peer, Math.max(0, currentNextIndex - 1));
            }
        }
    }

    public VoteResponse handleVoteRequest(VoteRequest request) {
        synchronized (stateLock) {
            boolean voteGranted = false;

            if (request.getTerm() > currentTerm) {
                becomeFollower(request.getTerm(), null);
            }

            if (request.getTerm() == currentTerm) {
                boolean logIsUpToDate = isCandidateLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm());
                if ((votedFor == null || votedFor.equals(NodeAddr.fromMsg(request.getCandidateId()))) && logIsUpToDate) {
                    votedFor = NodeAddr.fromMsg(request.getCandidateId());
                    voteGranted = true;
                    resetHeartbeatTimer();
                }
            }
            return VoteResponse.newBuilder()
                .setTerm(currentTerm)
                .setVoteGranted(voteGranted)
                .setFromNodeId(nodeId.toMsg())
                .build();
        }
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        synchronized (stateLock) {
            boolean success = false;
            
            if (request.getTerm() > currentTerm) {
                becomeFollower(request.getTerm(), NodeAddr.fromMsg(request.getLeaderId()));
            }

            if (request.getTerm() == currentTerm) {
                if(role == Role.CANDIDATE) becomeFollower(request.getTerm(), NodeAddr.fromMsg(request.getLeaderId()));
                
                resetHeartbeatTimer();
                currentLeader = NodeAddr.fromMsg(request.getLeaderId());

                // Cek konsistensi log
                int prevLogIndex = request.getPrevLogIndex();
                if (prevLogIndex == -1 || (prevLogIndex < log.size() && log.get(prevLogIndex).getTerm() == request.getPrevLogTerm())) {
                    success = true;
                    int logIndex = prevLogIndex + 1;
                    for(int i = 0; i < request.getEntriesCount(); i++) {
                        Entry newEntry = Entry.fromMsg(request.getEntries(i));
                        if(logIndex < log.size()) {
                            if(log.get(logIndex).getTerm() != newEntry.getTerm()) {
                                log.subList(logIndex, log.size()).clear(); // Truncate
                                log.add(newEntry);
                            }
                        } else {
                            log.add(newEntry);
                        }
                        logIndex++;
                    }
                    if (request.getLeaderCommit() > commitIndex) {
                        commitIndex = Math.min(request.getLeaderCommit(), log.size() - 1);
                    }
                }
            }

            return AppendEntriesResponse.newBuilder()
                .setTerm(currentTerm)
                .setSuccess(success)
                .setFromNodeId(nodeId.toMsg())
                .build();
        }
    }

    public CompletableFuture<String> handleClientRequest(String command, String key, String value) {
        synchronized (stateLock) {
            if (role != Role.LEADER) {
                CompletableFuture<String> future = new CompletableFuture<>();
                future.complete("REDIRECT:" + (currentLeader != null ? currentLeader.toString() : "UNKNOWN"));
                return future;
            }
            
            if (command.equals("add_node") || command.equals("remove_node")) {
                System.out.println("[" + nodeId + "] Received membership change command: " + command + " " + key);
            }

            Entry newEntry = new Entry(currentTerm, command, key, value);
            log.add(newEntry);
            int entryIndex = log.size() - 1;
            
            CompletableFuture<String> future = new CompletableFuture<>();
            pendingRequests.put(entryIndex, future);
            
            System.out.println("[" + nodeId + "] Accepted request for index " + entryIndex + ": " + newEntry);
            sendAppendEntriesToFollowers(false); 
            return future;
        }
    }


    // ====== Utility Methods ======
    private void applyCommittedEntries() {
        synchronized (stateLock) {
            while (lastApplied < commitIndex) {
                lastApplied++;
                if (lastApplied < log.size()) {
                    Entry entry = log.get(lastApplied);
                    String result = "OK";
                    String cmd = entry.getCommand();

                    if (cmd.equals("add_node")) {
                        NodeAddr newNode = NodeAddr.fromString(entry.getKey());
                        if (!clusterNodes.contains(newNode)) {
                            clusterNodes.add(newNode);
                            System.out.println("[" + nodeId + "] Applied config change: ADDED node " + newNode);
                        }
                    } else if (cmd.equals("remove_node")) {
                        NodeAddr oldNode = NodeAddr.fromString(entry.getKey());
                        if (clusterNodes.remove(oldNode)) {
                            System.out.println("[" + nodeId + "] Applied config change: REMOVED node " + oldNode);
                        }
                    } else {
                        result = kvStore.executeCommand(cmd, entry.getKey(), entry.getValue());
                    }

                    if (pendingRequests.containsKey(lastApplied)) {
                        pendingRequests.remove(lastApplied).complete(result);
                    }
                    System.out.println("[" + nodeId + "] Applied log[" + lastApplied + "]: " + entry + " -> " + result);
                }
            }
        }
    }
    
    private void updateCommitIndex() {
        int majority = clusterNodes.size() / 2; // N/2, bukan (N/2)+1
        for (int N = log.size() - 1; N > commitIndex; N--) {
            if (log.get(N).getTerm() == currentTerm) {
                long matchCount = 1; // Count self
                for (NodeAddr peer : clusterNodes) {
                    if (!peer.equals(nodeId) && matchIndex.getOrDefault(peer, -1) >= N) {
                        matchCount++;
                    }
                }
                if (matchCount > majority) {
                    commitIndex = N;
                    System.out.println("[" + nodeId + "][Term " + currentTerm + "] Commit index updated to " + commitIndex);
                    applyCommittedEntries(); 
                    break;
                }
            }
        }
    }

    private boolean isCandidateLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int myLastLogTerm = log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
        int myLastLogIndex = log.size() - 1;
        return candidateLastLogTerm > myLastLogTerm || (candidateLastLogTerm == myLastLogTerm && candidateLastLogIndex >= myLastLogIndex);
    }
    
    private boolean isElectionTimeout() {
        return System.currentTimeMillis() - lastHeartbeatTime > electionTimeout;
    }
    
    private void resetHeartbeatTimer() {
        lastHeartbeatTime = System.currentTimeMillis();
    }
    
    private long randomElectionTimeout() {
        return ELECTION_TIMEOUT_MIN_MS + new Random().nextInt((int)(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS));
    }
    
    public void shutdown() {
        running = false;
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                System.err.println("Scheduler did not terminate.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("[" + nodeId + "] Raft node shut down.");
    }

    // ====== Getters======
    public int getCurrentTerm() { return currentTerm; }
    public Role getRole() { return role; }
    public NodeAddr getNodeAdr() { return nodeId; }
    public NodeAddr getCurrentLeader() { return currentLeader; }
    public KVStore getKVStore() { return this.kvStore; }
    public boolean isLeader() { return this.role == Role.LEADER; }
    public List<String> getLogForClient() {
        synchronized(log) {
            return log.stream().map(Entry::toString).collect(Collectors.toList());
        }
    }
}