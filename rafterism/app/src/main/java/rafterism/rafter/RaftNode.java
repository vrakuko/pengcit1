package rafterism.rafter; // Package disesuaikan

// import rafterism.rafter.NodeAddr; // Menggunakan NodeAddr
// import rafterism.rafter.Entry;     // Menggunakan Entry
// import rafterism.rafter.KVStore;   // Menggunakan KVStore
// import rafterism.rafter.msg.NewEntryReq;
// import rafterism.rafter.msg.NewEntryResp;
// import rafterism.rafter.msg.  NewVoteReq;
// import rafterism.rafter.msg.NewVoteResp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class RaftNode {
    public enum Role {
        LEADER,
        FOLLOWER,
        CANDIDATE
    }

    // ====== Node Identity ======
    private final NodeAddr nodeId; // bisa IP:PORT atau ID unik
    private final List<NodeAddr> clusterNodes; // Peers di cluster, tidak termasuk diri sendiri
    private final Lock stateLock = new ReentrantLock(); // Untuk melindungi shared state

    // ====== Raft Core State (Persistent) ======
    private int currentTerm;
    private NodeAddr votedFor;
    private List<Entry> log; // Replicated log

    // ====== Raft Volatile State (on all servers) ======
    private int commitIndex; // highest log entry known to be committed
    private int lastApplied; // highest log entry applied to state machine

    private Role role;
    private NodeAddr currentLeader;

    // ====== Timing ======
    private long lastHeartbeatTime;
    private long electionTimeout; // Random timeout for followers/candidates
    private static final long HEARTBEAT_INTERVAL = 150; // ms (for leader heartbeats)

    // ====== Leader State (Volatile) ======
    // Reinitialized after election
    private Map<String, Integer> nextIndex = new ConcurrentHashMap<>(); // for each server, index of the next log entry to send to that server
    private Map<String, Integer> matchIndex = new ConcurrentHashMap<>(); // for each server, index of the highest log entry known to be replicated on server

    // ====== Application Layer ======
    private KVStore kvStore;

    // ===== Election Specifics =====
    private AtomicInteger votesReceived;
    private final Object electionLock = new Object(); // Masih ada, tapi stateLock lebih utama

    // ==== Concurrency Util =====
    private volatile boolean running;
    private ScheduledExecutorService scheduler; // Untuk menjalankan 'step' loop
    private ScheduledExecutorService rpcScheduler; // Untuk mengirim RPC secara konkruen

    // ====== Constructor ======
    // Default constructor untuk node yang pertama kali start sebagai FOLLOWER (atau leader jika tidak ada contact_addr)
    public RaftNode(NodeAddr nodeId, List<NodeAddr> initialClusterNodes) {
        this.nodeId = nodeId;
        // List clusterNodes hanya berisi node lain, tidak termasuk diri sendiri
        this.clusterNodes = initialClusterNodes.stream()
                                            .filter(node -> !node.equals(nodeId))
                                            .collect(Collectors.toList());
        this.currentTerm = 0; // Term awal
        this.votedFor = null; // Belum voting
        this.log = new ArrayList<>();
        this.commitIndex = -1; // Belum ada yang ter-commit
        this.lastApplied = -1; // Belum ada yang ter-apply
        this.role = Role.FOLLOWER;
        this.currentLeader = null;
        this.lastHeartbeatTime = System.currentTimeMillis();
        this.electionTimeout = randomElectionTimeout();
        this.kvStore = new KVStore();

        this.votesReceived = new AtomicInteger(0);
        this.running = true;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.rpcScheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

        printLog("Node initialized. Starting Raft lifecycle.");
        startRaftLifecycle(); // Memulai siklus Raft
    }

    // Constructor lengkap, berguna untuk restore state atau inisialisasi lebih spesifik (misal, leader awal)
    public RaftNode(NodeAddr nodeId, List<NodeAddr> clusterNodes, int currentTerm, Role role, NodeAddr votedFor, List<Entry> initialLog, int commitIdx, int lastApplied, NodeAddr currentLeader) {
        this.nodeId = nodeId;
        this.clusterNodes = clusterNodes.stream()
                                            .filter(node -> !node.equals(nodeId))
                                            .collect(Collectors.toList());
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.log = (initialLog != null) ? new ArrayList<>(initialLog) : new ArrayList<>(); // Deep copy
        this.commitIndex = commitIdx;
        this.lastApplied = lastApplied;
        this.role = role;
        this.currentLeader = currentLeader;

        this.lastHeartbeatTime = System.currentTimeMillis();
        this.electionTimeout = randomElectionTimeout();
        this.kvStore = new KVStore();
        // Penting: Jika memulihkan state dari log, KVStore harus di-apply ulang
        // Ini bisa dilakukan dengan memanggil applyLogEntries() setelah konstruksi,
        // namun perlu memastikan log sudah sesuai commitIndex.
        // Untuk simplicity di sini, kita anggap KVStore kosong dan akan diisi saat applyLogEntries berjalan.

        if (role == Role.LEADER) {
            initializeLeaderState();
        }
        this.votesReceived = new AtomicInteger(0);
        this.running = true;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.rpcScheduler = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

        printLog("Node initialized with specific state. Starting Raft lifecycle.");
        startRaftLifecycle();
    }

    // Helper untuk menginisialisasi nextIndex dan matchIndex saat menjadi Leader
    private void initializeLeaderState() {
        int lastLogIdx = log.isEmpty() ? -1 : log.size() - 1;
        for (NodeAddr peer : clusterNodes) {
            String peerId = peer.toString();
            // Next log entry to send to that server (initially, next after leader's last)
            nextIndex.put(peerId, lastLogIdx + 1);
            // Index of the highest log entry known to be replicated on that server (initially -1)
            matchIndex.put(peerId, -1);
        }
        printLog("Initialized leader state (nextIndex, matchIndex).");
    }


    private void startRaftLifecycle() {
        // Jalankan metode step secara berkala untuk memicu aksi Raft
        scheduler.scheduleAtFixedRate(this::step, 0, 50, TimeUnit.MILLISECONDS); // Setiap 50ms
        printLog("Raft lifecycle scheduler started.");
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

    public List<Entry> getLog() {
        return new ArrayList<>(log); // Mengembalikan salinan untuk mencegah modifikasi eksternal
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public int getLastApplied() {
        return lastApplied;
    }


    // Step: Main loop Raft yang dijalankan secara berkala
    private void step() {
        stateLock.lock(); // Kunci state untuk mencegah race condition
        try {
            if (role == Role.FOLLOWER) {
                if (isElectionTimeout()) {
                    printLog("Election timeout. Becoming CANDIDATE.");   
                    becomeCandidate();
                }
            } else if (role == Role.CANDIDATE) {
                if (isElectionTimeout()) {
                    printLog("Election timeout. Restarting election.");   
                    startElection(); // Mulai ulang pemilihan jika timeout terjadi saat kandidat
                }
            } else if (role == Role.LEADER) {
                if (System.currentTimeMillis() - lastHeartbeatTime >= HEARTBEAT_INTERVAL) {
                    sendHeartbeat();   
                    resetHeartbeatTimer();
                }
                advanceCommitIndex(); // Leader mencoba memajukan commitIndex
            }
            applyLogEntries(); // Terapkan entri yang sudah ter-commit ke KVStore
        } finally {
            stateLock.unlock(); // Lepas kunci
        }
    }

    // Update Role

    private void becomeFollower(int term, NodeAddr leaderId) {
        // Jika node menerima RPC dengan term yang lebih tinggi, atau jika ia adalah Leader/Candidate
        // dan menemukan Leader baru dengan term yang sama atau lebih tinggi, ia menjadi Follower.
        role = Role.FOLLOWER;
        currentTerm = term;
        votedFor = null; // Reset votedFor di term baru
        currentLeader = leaderId;
        resetHeartbeatTimer();
        electionTimeout = randomElectionTimeout(); // Set ulang election timeout
        printLog("Became FOLLOWER for term " + currentTerm + " (Leader: " + (leaderId != null ? leaderId.toString() : "None") + ").");
    }

    private void becomeCandidate() {
        role = Role.CANDIDATE;
        currentTerm++; // Increment term
        votedFor = nodeId; // Vote untuk diri sendiri
        votesReceived.set(1); // Mulai dengan 1 vote (dari diri sendiri)
        currentLeader = null; // Tidak ada Leader yang diketahui
        resetHeartbeatTimer(); // Reset timer untuk election timeout baru
        electionTimeout = randomElectionTimeout(); // Set election timeout baru

        printLog("Became CANDIDATE for term " + currentTerm + ". Starting election...");
        startElection();
    }

    private void becomeLeader() {
        role = Role.LEADER;
        currentLeader = nodeId;
        // Inisialisasi nextIndex dan matchIndex untuk setiap follower.
        // nextIndex diinisialisasi ke (last log index + 1) untuk setiap follower.
        initializeLeaderState();
        printLog("Became LEADER for term " + currentTerm + ".");
        sendHeartbeat(); // Kirim heartbeat segera setelah menjadi leader
        resetHeartbeatTimer();
    }

    // Election Logic

    private void startElection() {
        stateLock.lock();
        try {
            // Panggil becomeCandidate untuk memastikan state sudah benar
            // (term sudah di-increment, votedFor self, votesReceived 1, timer reset)
            becomeCandidate();

            int lastLogIndex = log.isEmpty() ? -1 : log.size() - 1;
            int lastLogTerm = log.isEmpty() ? 0 : log.get(lastLogIndex).getTerm();

            // Kirim RequestVote RPC ke semua node lain
            for (NodeAddr peer : clusterNodes) {
                // Gunakan rpcScheduler untuk mengirim RPC secara non-blocking
                rpcScheduler.submit(() -> {
                    try {
                        printLog("Sending   NewVoteReq to " + peer);
                          NewVoteReq request = new   NewVoteReq(currentTerm, nodeId, peer, lastLogIndex, lastLogTerm);

                        // --- SIMULASI RPC CALL ---
                        // Ini adalah placeholder. Dalam implementasi nyata, Anda akan menggunakan gRPC client di sini.
                        // Misalnya: RaftServiceGrpc.RaftServiceBlockingStub stub = RaftServiceGrpc.newBlockingStub(channel);
                        // NewVoteResp response = stub.requestVote(convertToJavaGrpcRequest(request));
                        // Untuk contoh ini, kita akan memanggil metode statis di Main untuk simulasi.
                        NewVoteResp response = Main.sendRequestVoteSimulated(request); // Panggil metode simulasi RPC di Main

                        stateLock.lock(); // Kunci state lagi untuk memproses respons
                        try {
                            // Rule 1: All servers, if RPC request or response contains a higher term:
                            // update currentTerm, convert to follower.
                            if (response.getTerm() > currentTerm) {
                                printLog("Received NewVoteResp with higher term (" + response.getTerm() + "). Becoming FOLLOWER.");
                                becomeFollower(response.getTerm(), null); // Leader is unknown
                                return;
                            }

                            // Hanya proses jika masih CANDIDATE dan term sama
                            if (role == Role.CANDIDATE && response.getTerm() == currentTerm) {
                                if (response.isVoteGranted()) {
                                    votesReceived.incrementAndGet();
                                    printLog("Received vote from " + response.getFrom() + ". Total votes: " + votesReceived.get() + " (out of " + getMajority() + " needed).");
                                    // Rule 2: If votes received from majority of servers: become leader.
                                    if (votesReceived.get() >= getMajority()) {
                                        if (role == Role.CANDIDATE) { // Cek lagi apakah masih kandidat
                                            printLog("Majority votes received! Becoming LEADER.");
                                            becomeLeader();
                                        }
                                    }
                                } else {
                                    printLog("Vote denied by " + response.getFrom() + " for term " + response.getTerm());
                                }
                            }
                        } finally {
                            stateLock.unlock();
                        }
                    } catch (Exception e) {
                        printLog("Error sending   NewVoteReq to " + peer + ": " + e.getMessage());
                        // Dalam sistem nyata, bisa jadi node ini tidak reachable, perlu penanganan lebih lanjut
                    }
                });
            }
        } finally {
            stateLock.unlock();
        }
    }

    // Menghitung jumlah mayoritas node (termasuk diri sendiri)
    private int getMajority() {
        return (clusterNodes.size() + 1) / 2 + 1; // +1 untuk diri sendiri
    }

    // RPC Handler: RequestVote
    // Dipanggil oleh kandidat lain yang meminta vote
    public NewVoteResp requestVote(  NewVoteReq request) {
        stateLock.lock();
        try {
            printLog("Received   NewVoteReq from " + request.getFrom() + " for term " + request.getTerm() +
                    ". My term: " + currentTerm + ", votedFor: " + (votedFor != null ? votedFor.toString() : "null"));

            boolean voteGranted = false;

            // 1. If RPC request or response contains a higher term:
            // update currentTerm, convert to follower.
            if (request.getTerm() > currentTerm) {
                printLog("Candidate's term (" + request.getTerm() + ") is higher. Updating term and becoming FOLLOWER.");
                becomeFollower(request.getTerm(), null);
            }

            // 2. If `request.term < currentTerm` reply false.
            if (request.getTerm() < currentTerm) {
                printLog("Vote denied: Candidate's term (" + request.getTerm() + ") < current term (" + currentTerm + ").");
                voteGranted = false;
            }
            // 3. If `votedFor` is null or `request.candidateId`:
            //    and candidate’s log is at least as up-to-date as receiver’s log, grant vote.
            else if (votedFor == null || votedFor.equals(request.getCandidateId())) {
                int lastLogIndex = log.isEmpty() ? -1 : log.size() - 1;
                int lastLogTerm = log.isEmpty() ? 0 : log.get(lastLogIndex).getTerm();

                // Raft "up-to-date" check (Raft Paper Figure 2, RequestVote RPC, Receiver implementation rules):
                // if (candidate's last log term > receiver's last log term) OR
                // (candidate's last log term == receiver's last log term AND candidate's last log index >= receiver's last log index)
                boolean candidateLogIsUpToDate =
                        request.getLastLogTerm() > lastLogTerm ||
                        (request.getLastLogTerm() == lastLogTerm && request.getLastLogIndex() >= lastLogIndex);

                if (candidateLogIsUpToDate) {
                    voteGranted = true;
                    votedFor = request.getCandidateId(); // Set votedFor
                    resetHeartbeatTimer(); // Reset timer saat memberikan vote
                    printLog("Vote granted to " + request.getCandidateId() + " for term " + request.getTerm());
                } else {
                    printLog("Vote denied: Candidate's log is not as up-to-date.");
                }
            } else {
                printLog("Vote denied: Already voted for " + votedFor + " in this term.");
            }

            return new NewVoteResp(currentTerm, voteGranted, nodeId, request.getFrom());
        } finally {
            stateLock.unlock();
        }
    }


    // RPC Handler: AppendEntries (termasuk Heartbeat)
    // Dipanggil oleh Leader untuk mereplikasi entri log atau sebagai heartbeat
    public NewEntryResp appendEntries(NewEntryReq request) {
        stateLock.lock();
        try {
            printLog("Received NewEntryReq from " + request.getFrom() + " for term " + request.getTerm() +
                    ", prevLogIdx: " + request.getPrevLogIdx() + ", entries: " + request.getEntries().size());

            // 1. Reply false if `request.term < currentTerm`
            if (request.getTerm() < currentTerm) {
                printLog("NewEntryReq denied: Leader's term (" + request.getTerm() + ") < current term (" + currentTerm + ").");
                return new NewEntryResp(currentTerm, false, nodeId, request.getFrom());
            }

            // Jika term Leader >= currentTerm, ini adalah Leader yang valid (atau calon Leader)
            // Reset timer dan update current leader.
            resetHeartbeatTimer();
            // Jika term leader lebih tinggi, atau jika node ini bukan follower, menjadi follower
            if (request.getTerm() > currentTerm || role != Role.FOLLOWER) {
                becomeFollower(request.getTerm(), request.getFrom());
            } else { // Jika term sama dan sudah follower, hanya update currentLeader (misal setelah network partition)
                currentLeader = request.getFrom();
            }

            // 2. Reply false if log doesn’t contain an entry at `prevLogIndex`
            //    whose term matches `prevLogTerm`.
            // prevLogIndex -1 artinya tidak ada previous log entry (misal, log kosong)
            if (request.getPrevLogIdx() >= 0 && log.size() <= request.getPrevLogIdx()) {
                printLog("NewEntryReq denied: prevLogIndex (" + request.getPrevLogIdx() + ") out of bounds (log size: " + log.size() + ").");
                return new NewEntryResp(currentTerm, false, nodeId, request.getFrom());
            }
            if (request.getPrevLogIdx() >= 0 && log.get(request.getPrevLogIdx()).getTerm() != request.getPrevLogTerm()) {
                printLog("NewEntryReq denied: prevLogTerm mismatch at index " + request.getPrevLogIdx() +
                        ". Expected: " + request.getPrevLogTerm() + ", Found: " + log.get(request.getPrevLogIdx()).getTerm());
                // Follower mungkin harus menghapus log dari index ini ke belakang
                // Simplified: Leader akan mencoba lagi dengan nextIndex yang lebih kecil
                return new NewEntryResp(currentTerm, false, nodeId, request.getFrom());
            }

            // 3. If an existing entry conflicts with a new one (same index but different terms),
            //    delete the existing entry and all that follow it.
            int conflictIndex = -1;
            for (int i = 0; i < request.getEntries().size(); i++) {
                int logIndex = request.getPrevLogIdx() + 1 + i;
                if (logIndex < log.size() && log.get(logIndex).getTerm() != request.getEntries().get(i).getTerm()) {
                    conflictIndex = logIndex;
                    break;
                }
            }
            if (conflictIndex != -1) {
                printLog("Log conflict detected at index " + conflictIndex + ". Truncating log.");
                log = new ArrayList<>(log.subList(0, conflictIndex));
            }

            // 4. Append any new entries not already in the log.
            // (Ini sudah ditangani oleh for loop di atas, karena jika ada konflik, log sudah di-truncate)
            for (Entry newEntry : request.getEntries()) {
                // Pastikan entry belum ada sebelum ditambahkan
                // Jika log di-truncate, maka semua entry baru dari Leader akan ditambahkan.
                // Jika tidak di-truncate, pastikan tidak menambahkan duplikat
                int entryIndex = request.getPrevLogIdx() + 1 + request.getEntries().indexOf(newEntry);
                if (entryIndex >= log.size()) { // Jika index melebihi ukuran log, berarti entry ini baru
                    log.add(newEntry);
                    printLog("Appended log entry: " + newEntry);
                } else {
                    // Jika entryIndex sudah ada, dan tidak ada konflik (term sama),
                    // berarti entry ini sudah ada dan tidak perlu ditambahkan lagi.
                }
            }


            // 5. If `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, index of last new entry)`.
            if (request.getLeaderCommit() > commitIndex) {
                int lastNewEntryIndex = log.isEmpty() ? -1 : log.size() - 1;
                commitIndex = Math.min(request.getLeaderCommit(), lastNewEntryIndex);
                printLog("Updated commitIndex to " + commitIndex);
                applyLogEntries(); // Segera apply perubahan ke state machine
            }

            return new NewEntryResp(currentTerm, true, nodeId, request.getFrom());
        } finally {
            stateLock.unlock();
        }
    }


    // Leader: Mengirim Heartbeat/AppendEntries ke semua follower
    private void sendHeartbeat() {
        if (role != Role.LEADER) return;

        printLog("[Leader] Sending heartbeat/AppendEntries RPCs.");

        for (NodeAddr peer : clusterNodes) {
            rpcScheduler.submit(() -> {
                stateLock.lock(); // Kunci state RaftNode saat mempersiapkan request
                try {
                    int nextIdx = nextIndex.getOrDefault(peer.toString(), log.size());
                    // Pastikan nextIdx tidak negatif
                    nextIdx = Math.max(0, nextIdx);

                    int prevLogIndex = nextIdx - 1;
                    int prevLogTerm = (prevLogIndex >= 0 && prevLogIndex < log.size()) ? log.get(prevLogIndex).getTerm() : 0;

                    List<Entry> entriesToSend = new ArrayList<>();
                    if (nextIdx < log.size()) { // Hanya kirim jika ada entri baru
                        entriesToSend = new ArrayList<>(log.subList(nextIdx, log.size()));
                    }

                    NewEntryReq request = new NewEntryReq(currentTerm, nodeId, prevLogIndex, prevLogTerm, entriesToSend, commitIndex, peer, nodeId);

                    // --- SIMULASI RPC CALL ---
                    // Panggil metode simulasi RPC di Main.
                    NewEntryResp response = Main.sendAppendEntriesSimulated(request);

                    stateLock.lock(); // Kunci state lagi untuk memproses respons
                    try {
                        if (role != Role.LEADER) return; // Jika peran berubah saat menunggu respons

                        // Rule 1: If RPC request or response contains a higher term:
                        // update currentTerm, convert to follower.
                        if (response.getTerm() > currentTerm) {
                            printLog("Received AppendEntriesResp with higher term (" + response.getTerm() + ") from " + peer + ". Becoming FOLLOWER.");
                            becomeFollower(response.getTerm(), null);
                            return;
                        }

                        // Jika berhasil (AppendEntries atau Heartbeat ACK)
                        if (response.isSuccess()) {
                            // Jika berhasil, update nextIndex dan matchIndex untuk follower ini
                            // matchIndex adalah indeks entry log tertinggi yang diketahui direplikasi.
                            matchIndex.put(peer.toString(), nextIdx + entriesToSend.size() - 1);
                            // nextIndex adalah indeks entry log berikutnya yang akan dikirim.
                            nextIndex.put(peer.toString(), nextIdx + entriesToSend.size());
                            printLog("AppendEntries to " + peer + " successful. nextIndex: " + nextIndex.get(peer.toString()) + ", matchIndex: " + matchIndex.get(peer.toString()));
                        } else {
                            // Jika gagal (misal, log tidak konsisten, atau prevLogIndex/Term salah)
                            // Leader decrement nextIndex dan retry.
                            nextIndex.compute(peer.toString(), (k, v) -> Math.max(0, v - 1)); // Decrement nextIndex, pastikan tidak negatif
                            printLog("AppendEntries to " + peer + " failed. Decrementing nextIndex to " + nextIndex.get(peer.toString()));
                            // Leader akan mencoba lagi di heartbeat berikutnya, atau bisa juga langsung retry
                        }
                    } finally {
                        stateLock.unlock();
                    }
                } catch (Exception e) {
                    printLog("Error sending AppendEntries to " + peer + ": " + e.getMessage());
                } finally {
                    // Pastikan lock dilepas meskipun ada exception saat menyiapkan request
                    stateLock.unlock();
                }
            });
        }
    }

    // Leader: Mencoba memajukan commitIndex
    private void advanceCommitIndex() {
        if (role != Role.LEADER) return;

        stateLock.lock();
        try {
            // Leader mencari N tertinggi sehingga log[N].term == currentTerm
            // dan log[N] direplikasi di mayoritas server.
            for (int N = log.size() - 1; N > commitIndex; N--) {
                if (log.get(N).getTerm() == currentTerm) { // Hanya komit entri dari term saat ini
                    int replicationCount = 1; // Termasuk diri sendiri (leader)
                    for (Map.Entry<String, Integer> entry : matchIndex.entrySet()) {
                        if (entry.getValue() >= N) {
                            replicationCount++;
                        }
                    }
                    if (replicationCount >= getMajority()) {
                        commitIndex = N;
                        printLog("Leader advanced commitIndex to " + commitIndex);
                        break; // Temukan N tertinggi, lalu keluar
                    }
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    // Menerapkan entri log yang sudah ter-commit ke state machine (KVStore)
    private void applyLogEntries() {
        stateLock.lock();
        try {
            while (lastApplied < commitIndex) {
                lastApplied++;
                Entry entryToApply = log.get(lastApplied);
                printLog("Applying log entry to KVStore: " + entryToApply);
                executeCommandOnKVStore(entryToApply);
            }
        } finally {
            stateLock.unlock();
        }
    }

    // Helper untuk mengeksekusi perintah pada KVStore dari sebuah Entry
    private void executeCommandOnKVStore(Entry entry) {
        // Menggunakan CommandType jika Anda mengimplementasikannya
        // else if (entry.getCommand().equalsIgnoreCase("SET"))
        switch (entry.getCommand().toUpperCase()) {
            case "SET":
                kvStore.set(entry.getKey(), entry.getValue());
                break;
            case "APPEND":
                kvStore.append(entry.getKey(), entry.getValue());
                break;
            case "DEL":
                kvStore.del(entry.getKey());
                break;
            // PING, GET, STRLEN tidak mengubah state, jadi tidak perlu di-apply dari log
            default:
                printLog("Unknown command in log entry, cannot apply: " + entry.getCommand());
                break;
        }
    }


    // Client RPCs: `execute` dan `request_log` (dipanggil oleh ClientServer)

    /**
     * Menangani permintaan klien untuk menjalankan perintah (ping, get, set, strLen, del, append).
     * Semua permintaan klien harus ke Leader. Jika bukan Leader, akan redirect.
     *
     * @param command Perintah (misal: "SET", "GET", "PING").
     * @param key Kunci untuk perintah. Bisa null.
     * @param value Nilai untuk perintah. Bisa null.
     * @return Respons String (misal: "OK", "PONG", nilai, pesan error).
     */
    public String execute(String command, String key, String value) {
        stateLock.lock();
        try {
            // Spesifikasi: "semua interface untuk client hanya dieksekusi jika request dikirimkan ke Leader. Selain itu tolak dan redirect" [cite: 185, 186, 188]
            if (role != Role.LEADER) {
                if (currentLeader != null) {
                    printLog("Client request for " + command + " redirected. Not leader. Current leader: " + currentLeader);
                    return "REDIRECT:" + currentLeader.getHost() + ":" + currentLeader.getPort();
                } else {
                    printLog("Client request for " + command + " denied. No leader known.");
                    return "ERROR: No leader known. Try again later.";
                }
            }

            // Jika Leader:
            // Perintah yang mengubah state (SET, APPEND, DEL) harus dicatat ke log Raft
            if (command.equalsIgnoreCase("SET") || command.equalsIgnoreCase("APPEND") || command.equalsIgnoreCase("DEL")) {
                Entry newEntry = new Entry(currentTerm, command.toUpperCase(), key, value); // Gunakan command.toUpperCase() untuk konsistensi
                log.add(newEntry);
                printLog("Leader proposed new log entry: " + newEntry + ". Current log size: " + log.size());

                // Di implementasi Raft yang sebenarnya, Leader akan mengirim AppendEntries
                // ke followers dan menunggu mayoritas ACK sebelum commit dan merespons OK.
                // Untuk demo, kita akan merespons OK segera setelah menambahkan ke log Leader,
                // dan asumsi replikasi akan terjadi di heartbeat berikutnya.
                // Metode `advanceCommitIndex` akan mengurus commit log dan `applyLogEntries` akan menerapkan ke KVStore.
                // Note: Ini adalah penyederhanaan yang bisa menyebabkan inconsistency sementara jika leader crash
                // sebelum log direplikasi mayoritas. Real Raft akan blocking sampai majority ACK.
                return "OK";
            }
            // Perintah read-only (PING, GET, STRLEN) dapat langsung dieksekusi dari KVStore
            // karena tidak mengubah state dan tidak perlu replikasi log.
            else if (command.equalsIgnoreCase("PING")) {
                printLog("Executing PING command.");
                return kvStore.ping();
            } else if (command.equalsIgnoreCase("GET")) {
                printLog("Executing GET command for key: " + key);
                return kvStore.get(key);
            } else if (command.equalsIgnoreCase("STRLEN")) { // Menggunakan STRLEN sesuai spec   
                printLog("Executing STRLEN command for key: " + key);
                return String.valueOf(kvStore.strLen(key));
            } else {
                printLog("Unknown client command: " + command);
                return "ERROR: Unknown command";
            }
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Mengembalikan seluruh log dari Leader node.
     *
     * @return List of String representasi dari log entries.
     */
    public List<String> requestLog() {
        stateLock.lock();
        try {
            // Spesifikasi: "semua interface untuk client hanya dieksekusi jika request dikirimkan ke Leader. Selain itu tolak dan redirect" [cite: 185, 186, 188]
            if (role != Role.LEADER) {
                if (currentLeader != null) {
                    printLog("Client request for log redirected. Not leader. Current leader: " + currentLeader);
                    return List.of("REDIRECT:" + currentLeader.getHost() + ":" + currentLeader.getPort());
                } else {
                    printLog("Client request for log denied. No leader known.");
                    return List.of("ERROR: No leader known. Try again later.");
                }
            }
            printLog("Returning current log to client.");
            return log.stream().map(Entry::toString).collect(Collectors.toList());
        } finally {
            stateLock.unlock();
        }
    }

    // Membership Change (Implementasi disederhanakan untuk contoh)
    // Spesifikasi menyebutkan "Membership Change" (Mekanisme untuk menambahkan dan menghapus node pada kluster berdasarkan perintah pengguna).   
    // Juga "Node yang mati tidak secara otomatis dikeluarkan dari cluster."   
    // Dan "Membership change (add member, remove member) dilakukan setelah kluster berjalan."   

    /**
     * Menambahkan node baru ke daftar cluster.
     * Dalam Raft sebenarnya, ini adalah entri log konfigurasi yang direplikasi.
     * @param newNodeId Alamat node baru yang akan ditambahkan.
     * @return True jika berhasil ditambahkan, false jika sudah ada atau invalid.
     */
    public boolean addClusterMember(NodeAddr newNodeId) {
        stateLock.lock();
        try {
            if (nodeId.equals(newNodeId)) {
                printLog("Cannot add self as cluster member.");
                return false;
            }
            if (!clusterNodes.contains(newNodeId)) {
                clusterNodes.add(newNodeId);
                // Jika saya adalah leader, saya perlu menginisialisasi nextIndex dan matchIndex untuk node baru ini
                if (role == Role.LEADER) {
                    int lastLogIdx = log.isEmpty() ? -1 : log.size() - 1;
                    nextIndex.put(newNodeId.toString(), lastLogIdx + 1);
                    matchIndex.put(newNodeId.toString(), -1);
                }
                printLog("Added new cluster member: " + newNodeId + ". Current cluster: " + clusterNodes);
                return true;
            }
            printLog("Node " + newNodeId + " is already a cluster member.");
            return false;
        } finally {
            stateLock.unlock();
        }
    }

    /**
     * Menghapus node dari daftar cluster.
     * Dalam Raft sebenarnya, ini adalah entri log konfigurasi. Sangat kompleks.
     * Untuk demo ini, hanya hapus dari daftar peers.
     * @param nodeToRemove Alamat node yang akan dihapus.
     * @return True jika berhasil dihapus, false jika tidak ditemukan.
     */
    public boolean removeClusterMember(NodeAddr nodeToRemove) {
        stateLock.lock();
        try {
            if (nodeId.equals(nodeToRemove)) {
                printLog("Cannot remove self from cluster member list.");
                return false;
            }
            boolean removed = clusterNodes.remove(nodeToRemove);
            if (removed) {
                // Jika saya adalah leader, saya juga perlu menghapus nextIndex dan matchIndex mereka
                if (role == Role.LEADER) {
                    nextIndex.remove(nodeToRemove.toString());
                    matchIndex.remove(nodeToRemove.toString());
                }
                printLog("Removed cluster member: " + nodeToRemove + ". Current cluster: " + clusterNodes);
            } else {
                printLog("Node " + nodeToRemove + " not found in cluster members.");
            }
            return removed;
        } finally {
            stateLock.unlock();
        }
    }


    // Dummy RPC simulations for testing within a single process.
    // Ini adalah placeholder. Dalam aplikasi nyata, Anda akan mengirim ini melalui jaringan gRPC.
    // Metode ini akan dipanggil oleh RaftNode itu sendiri untuk berkomunikasi dengan node lain.
    // Asumsi: Main.java akan menyediakan static helper method untuk melakukan panggilan simulated RPC ini.
    // Anda harus memastikan Main.java memiliki metode `sendRequestVoteSimulated` dan `sendAppendEntriesSimulated`.
    // (Ini sudah saya sertakan di Main.java yang saya berikan sebelumnya).
    // Implementasi nyata akan menggunakan gRPC client dan server.

    // Utility untuk logging dengan timestamp dan ID node
    private void printLog(String message) {
        System.out.println(String.format("[%s] [%s] %s", nodeId.toString(),
                new java.text.SimpleDateFormat("HH:mm:ss.SSS").format(new java.util.Date()), message));
    }

    // Metode shutdown untuk penghentian yang bersih
    public void shutdown() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt(); // Restore interrupt status
            }
        }
        if (rpcScheduler != null) {
            rpcScheduler.shutdown();
            try {
                if (!rpcScheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                    rpcScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                rpcScheduler.shutdownNow();
                Thread.currentThread().interrupt(); // Restore interrupt status
            }
        }
        printLog("Node " + nodeId + " shutting down.");
    }
}