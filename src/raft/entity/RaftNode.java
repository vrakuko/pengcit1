package raft.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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

    // node identity
    private final Alamat nodeId;
    private final List<Alamat> clusterNodes;

    // raft core state
    private volatile int currentTerm;
    private volatile Alamat votedFor;
    private final List<Entry> log;
    private volatile int commitIndex;
    private volatile int lastApplied;
    private volatile Role role;
    private volatile Alamat currentLeader;

    // timing
    private volatile long lastHeartbeatTime;
    private volatile long electionTimeout;
    private static final long HEARTBEAT_INTERVAL = 1000; // 1 second
    private static final long ELECTION_TIMEOUT_MIN = 2000; // 2 seconds
    private static final long ELECTION_TIMEOUT_MAX = 3000; // 3 seconds

    // leader state
    private final Map<String, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();

    private final KVStore kvStore;
    private final AtomicInteger votesReceived;
    private final Object stateLock = new Object();
    private volatile boolean running;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> electionTask;
    private ScheduledFuture<?> heartbeatTask;
    private RPCServer rpcServer;
    private ClientServer clientServer;

    public RaftNode() {
        this.nodeId = new Alamat("localhost", 8000);
        this.clusterNodes = new ArrayList<>();
        this.clusterNodes.add(nodeId);
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = new ArrayList<>();
        this.commitIndex = -1;
        this.lastApplied = -1;
        this.role = Role.FOLLOWER;
        this.currentLeader = null;
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.electionTimeout = randomElectionTimeout();
        this.kvStore = new KVStore();
        this.votesReceived = new AtomicInteger(0);
        this.running = true;
        this.scheduler = Executors.newScheduledThreadPool(3);
        
        startRaftLifecycle();
    }

    public RaftNode(Alamat nodeId, List<Alamat> clusterNodes) {
        this.nodeId = nodeId;
        this.clusterNodes = new ArrayList<>(clusterNodes);
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = new ArrayList<>();
        this.commitIndex = -1;
        this.lastApplied = -1;
        this.role = Role.FOLLOWER;
        this.currentLeader = null;
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.electionTimeout = randomElectionTimeout();
        this.kvStore = new KVStore();
        this.votesReceived = new AtomicInteger(0);
        this.running = true;
        this.scheduler = Executors.newScheduledThreadPool(3);
        
        startRaftLifecycle();
    }

    private void startRaftLifecycle() {
        scheduler.scheduleAtFixedRate(this::step, 100, 100, TimeUnit.MILLISECONDS);
        
        // init server
        this.rpcServer = new RPCServer(this);
        this.clientServer = new ClientServer(this);
    }

    // election timeout
    private long randomElectionTimeout() {
        Random random = new Random();
        return ELECTION_TIMEOUT_MIN + random.nextInt((int)(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN));
    }

    // timer update
    public void resetHeartbeatTimer() {
        lastHeartbeatTime = System.currentTimeMillis();
    }

    public boolean isElectionTimeout() {
        return System.currentTimeMillis() - lastHeartbeatTime > electionTimeout;
    }

    // getters
    public int getCurrentTerm() { return currentTerm; }
    public Alamat getVotedFor() { return votedFor; }
    public Role getRole() { return role; }
    public Alamat getNodeAdr() { return nodeId; }
    public Alamat getCurrentLeader() { return currentLeader; }
    public KVStore getKVStore() { return kvStore; }
    public List<Entry> getLog() { return new ArrayList<>(log); }
    public boolean isLeader() { return role == Role.LEADER; }
    public int getCommitIndex() { return commitIndex; }

    // main state machine loop
    private void step() {
        if (!running) return;

        try {
            synchronized (stateLock) {
                switch (role) {
                    case FOLLOWER:
                        if (isElectionTimeout()) {
                            System.out.println("[" + nodeId + "] Election timeout, becoming candidate");
                            becomeCandidate();
                        }
                        break;
                    case CANDIDATE:
                        if (isElectionTimeout()) {
                            System.out.println("[" + nodeId + "] Election timeout, starting new election");
                            startElection();
                        }
                        break;
                    case LEADER:
                        if (System.currentTimeMillis() - lastHeartbeatTime >= HEARTBEAT_INTERVAL) {
                            sendHeartbeat();
                            resetHeartbeatTimer();
                        }
                        break;
                }
            }
        } catch (Exception e) {
            System.err.println("[" + nodeId + "] Error in step: " + e.getMessage());
        }
    }

    // role transitions
    private void becomeFollower(int term, Alamat leader) {
        synchronized (stateLock) {
            if (term > currentTerm) {
                currentTerm = term;
                votedFor = null;
            }
            
            role = Role.FOLLOWER;
            currentLeader = leader;
            cancelHeartbeatTask();
            resetElectionTimeout();
            
            System.out.println("[" + nodeId + "] Became FOLLOWER for term " + currentTerm + 
                             (leader != null ? ", leader: " + leader : ""));
        }
    }

    private void becomeCandidate() {
        synchronized (stateLock) {
            role = Role.CANDIDATE;
            currentTerm++;
            votedFor = nodeId;
            currentLeader = null;
            votesReceived.set(1); // vote diri sendiri
            resetElectionTimeout();
            
            System.out.println("[" + nodeId + "] Became CANDIDATE for term " + currentTerm);
            startElection();
        }
    }

    private void becomeLeader() {
        synchronized (stateLock) {
            role = Role.LEADER;
            currentLeader = nodeId;
            cancelElectionTask();
            
            nextIndex.clear();
            matchIndex.clear();
            
            int nextLogIndex = log.size();
            for (Alamat peer : clusterNodes) {
                if (!peer.equals(nodeId)) {
                    nextIndex.put(peer.toString(), nextLogIndex);
                    matchIndex.put(peer.toString(), -1);
                }
            }
            System.out.println("[" + nodeId + "] Became LEADER for term " + currentTerm);
            sendHeartbeat();
            startHeartbeatTask();
        }
    }

    // election
    private void startElection() {
        if (role != Role.CANDIDATE) return;

        System.out.println("[" + nodeId + "] Starting election for term " + currentTerm);
        
        int lastLogIndex = log.size() - 1;
        int lastLogTerm = lastLogIndex >= 0 ? log.get(lastLogIndex).getTerm() : 0;

        for (Alamat peer : clusterNodes) {
            if (!peer.equals(nodeId)) {
                VoteReq voteReq = new VoteReq(currentTerm, nodeId, peer, lastLogIndex, lastLogTerm);
                CompletableFuture.supplyAsync(() -> sendVoteRequest(peer, voteReq))
                    .thenAccept(this::handleVoteResponse)
                    .exceptionally(throwable -> {
                        System.err.println("[" + nodeId + "] Vote request to " + peer + " failed: " + throwable.getMessage());
                        return null;
                    });
            }
        }
        checkElectionResult();
    }

    private void checkElectionResult() {
        synchronized (stateLock) {
            if (role != Role.CANDIDATE) return;
            
            int majority = (clusterNodes.size() / 2) + 1;
            if (votesReceived.get() >= majority) {
                becomeLeader();
            }
        }
    }

    // heartbeat
    private void sendHeartbeat() {
        if (role != Role.LEADER) return;

        System.out.println("[" + nodeId + "] Sending heartbeat to " + (clusterNodes.size() - 1) + " followers");
        
        for (Alamat peer : clusterNodes) {
            if (!peer.equals(nodeId)) {
                sendAppendEntries(peer, new ArrayList<>());
            }
        }
    }

    private void sendAppendEntries(Alamat target, List<Entry> entries) {
        int prevLogIndex = nextIndex.getOrDefault(target.toString(), log.size()) - 1;
        int prevLogTerm = prevLogIndex >= 0 && prevLogIndex < log.size() ? 
                         log.get(prevLogIndex).getTerm() : 0;

        NewEntryReq request = new NewEntryReq(
            currentTerm, nodeId, prevLogIndex, prevLogTerm, 
            entries, commitIndex, target, nodeId
        );

        CompletableFuture.supplyAsync(() -> sendAppendEntriesRequest(target, request))
            .thenAccept(response -> handleAppendEntriesResponse(target, request, response))
            .exceptionally(throwable -> {
                System.err.println("[" + nodeId + "] AppendEntries to " + target + " failed: " + throwable.getMessage());
                return null;
            });
    }

    // rpc handlers
    public VoteResp handleVoteRequest(VoteReq request) {
        synchronized (stateLock) {
            boolean voteGranted = false;

            if (request.getTerm() > currentTerm) {
                currentTerm = request.getTerm();
                votedFor = null;
                becomeFollower(request.getTerm(), null);
            }
            if (request.getTerm() == currentTerm &&
                (votedFor == null || votedFor.equals(request.getFrom())) &&
                isLogUpToDate(request.getLastLogIndex(), request.getLastLogTerm())) {
                
                voteGranted = true;
                votedFor = request.getFrom();
                resetHeartbeatTimer();
                
                System.out.println("[" + nodeId + "] Granted vote to " + request.getFrom() + 
                                 " for term " + request.getTerm());
            } else {
                System.out.println("[" + nodeId + "] Denied vote to " + request.getFrom() + 
                                 " for term " + request.getTerm());
            }
            
            return new VoteResp(currentTerm, voteGranted, nodeId, request.getFrom());
        }
    }

    public NewEntryResp handleAppendEntries(NewEntryReq request) {
        synchronized (stateLock) {
            boolean success = false;
            
            if (request.getTerm() > currentTerm) {
                currentTerm = request.getTerm();
                votedFor = null;
            }
            if (request.getTerm() >= currentTerm) {
                becomeFollower(request.getTerm(), request.getFrom());
                resetHeartbeatTimer();
                if (request.getPrevLogIdx() == -1 || (request.getPrevLogIdx() < log.size() && request.getPrevLogIdx() >= 0 && log.get(request.getPrevLogIdx()).getTerm() == request.getPrevLogTerm())) {
                    success = true;
                    if (request.getEntries() != null && !request.getEntries().isEmpty()) {
                        int logIndex = request.getPrevLogIdx() + 1;
                        while (logIndex < log.size()) {
                            log.remove(logIndex);
                        }
                        
                        for (Entry entry : request.getEntries()) {
                            entry.setIndex(logIndex++);
                            log.add(entry);
                        }
                        System.out.println("[" + nodeId + "] Appended " + request.getEntries().size() + " entries from leader");
                    }
                    if (request.getLeaderCommit() > commitIndex) {
                        commitIndex = Math.min(request.getLeaderCommit(), log.size() - 1);
                        applyLogEntries();
                    }
                }
            }
            return new NewEntryResp(currentTerm, success, nodeId, request.getFrom());
        }
    }

    // command execution
    public String executeCommand(String command, String key, String value) throws Exception {
        synchronized (stateLock) {
            if (role != Role.LEADER) {
                if (currentLeader != null) {
                    return "REDIRECT:" + currentLeader.toString();
                } else {
                    throw new Exception("No leader available");
                }
            }
            
            Entry entry = new Entry(currentTerm, log.size(), command, key, value);
            
            if (!entry.isValid()) {
                throw new IllegalArgumentException("Invalid command");
            }
            log.add(entry);
            if (clusterNodes.size() == 1) {
                commitIndex = entry.getIndex();
                applyLogEntries();
                return kvStore.executeCommand(entry);
            }
            if (replicateToMajority(entry)) {
                commitIndex = entry.getIndex();
                applyLogEntries();
                return kvStore.executeCommand(entry);
            } else {
                log.remove(log.size() - 1);
                throw new Exception("Failed to replicate to majority");
            }
        }
    }

    private boolean isLogUpToDate(int candidateLastLogIndex, int candidateLastLogTerm) {
        int myLastLogTerm = log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
        int myLastLogIndex = log.size() - 1;
        
        return candidateLastLogTerm > myLastLogTerm ||
               (candidateLastLogTerm == myLastLogTerm && candidateLastLogIndex >= myLastLogIndex);
    }

    private void applyLogEntries() {
        while (lastApplied < commitIndex && lastApplied + 1 < log.size()) {
            lastApplied++;
            Entry entry = log.get(lastApplied);
            try {
                kvStore.executeCommand(entry);
                System.out.println("[" + nodeId + "] Applied entry: " + entry);
            } catch (Exception e) {
                System.err.println("[" + nodeId + "] Failed to apply entry " + entry + ": " + e.getMessage());
            }
        }
    }

    private boolean replicateToMajority(Entry entry) {
        return clusterNodes.size() == 1;
    }

    private void resetElectionTimeout() {
        electionTimeout = randomElectionTimeout();
        lastHeartbeatTime = System.currentTimeMillis();
        
        cancelElectionTask();
        if (role != Role.LEADER) {
            electionTask = scheduler.schedule(this::handleElectionTimeout, electionTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private void handleElectionTimeout() {
        synchronized (stateLock) {
            if (role == Role.FOLLOWER || role == Role.CANDIDATE) {
                becomeCandidate();
            }
        }
    }

    private void startHeartbeatTask() {
        cancelHeartbeatTask();
        if (role == Role.LEADER) {
            heartbeatTask = scheduler.scheduleAtFixedRate(
                this::sendHeartbeat, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS
            );
        }
    }

    private void cancelElectionTask() {
        if (electionTask != null && !electionTask.isCancelled()) {
            electionTask.cancel(false);
        }
    }

    private void cancelHeartbeatTask() {
        if (heartbeatTask != null && !heartbeatTask.isCancelled()) {
            heartbeatTask.cancel(false);
        }
    }

    private VoteResp sendVoteRequest(Alamat target, VoteReq request) {
        return new VoteResp(currentTerm, false, target, nodeId);
    }

    private NewEntryResp sendAppendEntriesRequest(Alamat target, NewEntryReq request) {
        return new NewEntryResp(currentTerm, true, target, nodeId);
    }

    private void handleVoteResponse(VoteResp response) {
        synchronized (stateLock) {
            if (role != Role.CANDIDATE || response.getTerm() != currentTerm) {
                return;
            }
            
            if (response.getTerm() > currentTerm) {
                becomeFollower(response.getTerm(), null);
                return;
            }
            
            if (response.isVoteGranted()) {
                int votes = votesReceived.incrementAndGet();
                System.out.println("[" + nodeId + "] Received vote from " + response.getFrom() + 
                                 ", total votes: " + votes);
                checkElectionResult();
            }
        }
    }

    private void handleAppendEntriesResponse(Alamat target, NewEntryReq request, NewEntryResp response) {
        synchronized (stateLock) {
            if (role != Role.LEADER || response.getTerm() != currentTerm) {
                return;
            }
            
            if (response.getTerm() > currentTerm) {
                becomeFollower(response.getTerm(), null);
                return;
            }
            
            String targetId = target.toString();
            
            if (response.isSuccess()) {
                int newMatchIndex = request.getPrevLogIdx() + 
                                  (request.getEntries() != null ? request.getEntries().size() : 0);
                matchIndex.put(targetId, newMatchIndex);
                nextIndex.put(targetId, newMatchIndex + 1);
            } else {
                int currentNext = nextIndex.getOrDefault(targetId, log.size());
                nextIndex.put(targetId, Math.max(0, currentNext - 1));
            }
        }
    }

    public boolean addNode(Alamat newNode) {
        synchronized (stateLock) {
            if (role != Role.LEADER) {
                return false;
            }
            
            if (clusterNodes.contains(newNode)) {
                return true;
            }
            
            clusterNodes.add(newNode);
            nextIndex.put(newNode.toString(), log.size());
            matchIndex.put(newNode.toString(), -1);
            
            System.out.println("[" + nodeId + "] Added node " + newNode + " to cluster");
            return true;
        }
    }

    public boolean removeNode(Alamat nodeToRemove) {
        synchronized (stateLock) {
            if (role != Role.LEADER) {
                return false;
            }
            
            if (!clusterNodes.contains(nodeToRemove)) {
                return true;
            }
            
            clusterNodes.remove(nodeToRemove);
            nextIndex.remove(nodeToRemove.toString());
            matchIndex.remove(nodeToRemove.toString());
            
            System.out.println("[" + nodeId + "] Removed node " + nodeToRemove + " from cluster");
            return true;
        }
    }

    public void startServers(int rpcPort, int clientPort) {
        rpcServer.start(rpcPort);
        clientServer.start(clientPort);
    }

    public void shutdown() {
        running = false;
        
        if (rpcServer != null) {
            rpcServer.shutdown();
        }
        
        if (clientServer != null) {
            clientServer.shutdown();
        }
        cancelElectionTask();
        cancelHeartbeatTask();
        
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }
        
        System.out.println("[" + nodeId + "] Raft node shutdown complete");
    }
}