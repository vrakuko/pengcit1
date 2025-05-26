package src.main.java.com.objek;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;




public class RaftNode {
    public enum NodeState {LEADER, CANDIDATE, FOLLOWER};
    public enum CommandType { PING, GET, SET, STRLEN, DEL, APPEND };
    // Configuration constants
    private static final int HEARTBEAT_INTERVAL_MS = 1000;
    private static final int ELECTION_TIMEOUT_MIN_MS = 2000;
    private static final int ELECTION_TIMEOUT_MAX_MS = 3000;
    private static final int RPC_TIMEOUT_MS = 500;
    
    // Node state
    private final Address nodeAddress;
    private final KVStore kvStore;
    private volatile NodeState state = NodeState.FOLLOWER;
    
    // Raft persistent state
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private volatile Address votedFor = null;
    private final List<LogEntry> log = new ArrayList<>();
    
    // Raft volatile state
    private volatile int commitIndex = 0;
    private volatile int lastApplied = 0;
    
    // Leader volatile state
    private final Map<Address, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<Address, Integer> matchIndex = new ConcurrentHashMap<>();
    
    // Cluster configuration
    private final Set<Address> clusterNodes = ConcurrentHashMap.newKeySet();
    private volatile Address currentLeader = null;
    
    // Threading
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledFuture<?> electionTask;
    
    // RPC components
    private RaftRPCServer rpcServer;
    private ClientServer clientServer;
    
    public RaftNode(Address nodeAddress, KVStore kvStore, Address contactAddress) {
        this.nodeAddress = nodeAddress;
        this.kvStore = kvStore;
        this.clusterNodes.add(nodeAddress);
        
        // Initialize RPC servers
        this.rpcServer = new RaftRPCServer(this);
        this.clientServer = new ClientServer(this);
        
        if (contactAddress == null) {
            // Initialize as single-node cluster (auto-leader)
            initializeAsLeader();
        } else {
            // Try to join existing cluster
            tryJoinCluster(contactAddress);
        }
        
        // Start servers
        startServers();
        
        // Start election timer
        resetElectionTimer();
    }
    
    private void initializeAsLeader() {
        printLog("Initializing as leader node...");
        this.state = NodeState.LEADER;
        this.currentLeader = this.nodeAddress;
        startHeartbeat();
    }
    
    private void tryJoinCluster(Address contactAddress) {
        printLog("Trying to join cluster via " + contactAddress);
        // TODO: Implement membership application
        // For now, just add to cluster and become follower
        this.clusterNodes.add(contactAddress);
        this.state = NodeState.FOLLOWER;
    }
    
    private void startServers() {
        executor.submit(() -> rpcServer.start(nodeAddress.getPort()));
        executor.submit(() -> clientServer.start(nodeAddress.getPort() + 1000)); // Client port = node port + 1000
    }
    
    // Heartbeat mechanism
    private void startHeartbeat() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        
        heartbeatTask = scheduler.scheduleAtFixedRate(() -> {
            if (state == NodeState.LEADER) {
                printLog("[LEADER] Sending heartbeat to followers...");
                sendHeartbeatToFollowers();
            }
        }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }
    
    private void sendHeartbeatToFollowers() {
        for (Address follower : clusterNodes) {
            if (!follower.equals(nodeAddress)) {
                executor.submit(() -> sendHeartbeatToNode(follower));
            }
        }
    }
    
    private void sendHeartbeatToNode(Address target) {
        try {
            // TODO: Implement actual RPC call
            printLog("Sending heartbeat to " + target);
            
            // Simulate heartbeat response
            // In real implementation, this would be an RPC call
            // HeartbeatResponse response = rpcClient.sendHeartbeat(target, ...);
            
        } catch (Exception e) {
            printLog("Failed to send heartbeat to " + target + ": " + e.getMessage());
        }
    }
    
    // Election mechanism
    private void resetElectionTimer() {
        if (electionTask != null) {
            electionTask.cancel(false);
        }
        
        // Random election timeout to prevent split votes
        int timeout = ELECTION_TIMEOUT_MIN_MS + 
                     new Random().nextInt(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS);
        
        electionTask = scheduler.schedule(() -> {
            if (state != NodeState.LEADER) {
                startElection();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }
    
    private void startElection() {
        printLog("[CANDIDATE] Starting election for term " + (currentTerm.get() + 1));
        
        // Become candidate
        state = NodeState.CANDIDATE;
        currentTerm.incrementAndGet();
        votedFor = nodeAddress;
        
        // Vote for self
        AtomicInteger voteCount = new AtomicInteger(1);
        int requiredVotes = (clusterNodes.size() / 2) + 1;
        
        printLog("[CANDIDATE] Need " + requiredVotes + " votes, got 1 (self)");
        
        // Request votes from other nodes
        for (Address node : clusterNodes) {
            if (!node.equals(nodeAddress)) {
                executor.submit(() -> requestVoteFromNode(node, voteCount, requiredVotes));
            }
        }
        
        // Reset election timer
        resetElectionTimer();
    }
    
    private void requestVoteFromNode(Address target, AtomicInteger voteCount, int requiredVotes) {
        try {
            // TODO: Implement actual RPC call
            printLog("Requesting vote from " + target);
            
            // Simulate vote response
            // In real implementation, this would be an RPC call
            boolean voteGranted = true; // Simulate successful vote
            
            if (voteGranted) {
                int currentVotes = voteCount.incrementAndGet();
                printLog("[CANDIDATE] Received vote from " + target + 
                        " (total: " + currentVotes + "/" + requiredVotes + ")");
                
                if (currentVotes >= requiredVotes && state == NodeState.CANDIDATE) {
                    becomeLeader();
                }
            }
            
        } catch (Exception e) {
            printLog("Failed to request vote from " + target + ": " + e.getMessage());
        }
    }
    
    private void becomeLeader() {
        printLog("[LEADER] Won election for term " + currentTerm.get());
        
        state = NodeState.LEADER;
        currentLeader = nodeAddress;
        
        // Initialize leader state
        for (Address node : clusterNodes) {
            if (!node.equals(nodeAddress)) {
                nextIndex.put(node, log.size());
                matchIndex.put(node, 0);
            }
        }
        
        // Start sending heartbeats
        startHeartbeat();
    }
    
    // Log replication
    public synchronized String executeCommand(String command, String key, String value) {
        if (state != NodeState.LEADER) {
            return "ERROR: Not leader. Current leader: " + 
                   (currentLeader != null ? currentLeader.toString() : "unknown");
        }
        
        // Handle read-only commands locally
        if ("get".equals(command) || "ping".equals(command) || "strlen".equals(command)) {
            return executeReadOnlyCommand(command, key);
        }
        
        // Create new log entry
        LogEntry entry = new LogEntry(currentTerm.get(), command, key, value);
        log.add(entry);
        
        printLog("[LEADER] Added log entry: " + entry);
        
        // Replicate to followers
        replicateLogEntry(entry);
        
        return "OK";
    }
    
    private String executeReadOnlyCommand(String command, String key) {
        switch (command) {
            case "get":
                return "\"" + kvStore.get(key) + "\"";
            case "ping":
                return kvStore.ping() ? "PONG" : "ERROR";
            case "strlen":
                return String.valueOf(kvStore.strlen(key));
            default:
                return "ERROR: Unknown command";
        }
    }
    
    private void replicateLogEntry(LogEntry entry) {
        // TODO: Implement log replication to followers
        printLog("[LEADER] Replicating log entry to followers...");
        
        // For now, just apply locally
        applyLogEntry(entry);
    }
    
    private void applyLogEntry(LogEntry entry) {
        applyLogEntry(entry);
        lastApplied++;
        printLog("Applied log entry: " + entry);
    }
    
    // Membership changes
    public synchronized boolean addNode(Address newNode) {
        if (state != NodeState.LEADER) {
            return false;
        }
        
        printLog("[LEADER] Adding node to cluster: " + newNode);
        clusterNodes.add(newNode);
        nextIndex.put(newNode, log.size());
        matchIndex.put(newNode, 0);
        
        // TODO: Implement proper membership change protocol
        return true;
    }
    
    public synchronized boolean removeNode(Address nodeToRemove) {
        if (state != NodeState.LEADER) {
            return false;
        }
        
        printLog("[LEADER] Removing node from cluster: " + nodeToRemove);
        clusterNodes.remove(nodeToRemove);
        nextIndex.remove(nodeToRemove);
        matchIndex.remove(nodeToRemove);
        
        // TODO: Implement proper membership change protocol
        return true;
    }
    
    // RPC handlers (called by RaftRPCServer)
    public VoteResponse handleVoteRequest(VoteRequest request) {
        printLog("Received vote request from " + request.getCandidateId() + 
                " for term " + request.getTerm());
        
        boolean voteGranted = false;
        
        // Check if we should grant vote
        if (request.getTerm() > currentTerm.get()) {
            currentTerm.set(request.getTerm());
            votedFor = null;
            state = NodeState.FOLLOWER;
        }
        
        if (request.getTerm() == currentTerm.get() && 
            (votedFor == null || votedFor.equals(request.getCandidateId()))) {
            // TODO: Check log completeness
            voteGranted = true;
            votedFor = request.getCandidateId();
            resetElectionTimer();
        }
        
        printLog("Vote " + (voteGranted ? "granted" : "denied") + 
                " to " + request.getCandidateId());
        
        return new VoteResponse(currentTerm.get(), voteGranted);
    }
    
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        printLog("Received append entries from " + request.getLeaderId());
        
        // Reset election timer - we heard from leader
        resetElectionTimer();
        
        // Update current leader
        currentLeader = request.getLeaderId();
        
        // TODO: Implement log consistency check and append logic
        
        return new AppendEntriesResponse(currentTerm.get(), true);
    }
    
    // Utility methods
    public List<LogEntry> getLog() {
        return new ArrayList<>(log);
    }
    
    public NodeState getState() {
        return state;
    }
    
    public Address getCurrentLeader() {
        return currentLeader;
    }
    
    public int getCurrentTerm() {
        return currentTerm.get();
    }
    
    public Set<Address> getClusterNodes() {
        return new HashSet<>(clusterNodes);
    }
    
    private void printLog(String message) {
        String timestamp = java.time.LocalTime.now().toString();
        System.out.printf("[%s] [%s] [%s] %s%n", 
                         nodeAddress, timestamp, state, message);
    }

    public void applyLogEntry(CommandType type, String key, String value) {
        try {
            switch (type) {
                case PING : kvStore.ping();
                case GET : kvStore.get(key);
                case SET : kvStore.set(key, value);
                case STRLEN : kvStore.strLen(key);
                case DEL : kvStore.del(key);
                case APPEND : kvStore.append(key, value);
                default : System.out.println("ERROR: Invalid command");
            };
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
        }
    }
    
    public void shutdown() {
        printLog("Shutting down node...");
        
        if (heartbeatTask != null) heartbeatTask.cancel(false);
        if (electionTask != null) electionTask.cancel(false);
        
        scheduler.shutdown();
        executor.shutdown();
        
        if (rpcServer != null) rpcServer.shutdown();
        if (clientServer != null) clientServer.shutdown();
    }
}