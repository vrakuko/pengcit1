package test;

// File: RaftTestMain.java

import raft.entity.*;
import raft.msg.*;

import java.util.*;

public class RaftTestMain {
    public static void main(String[] args) {
        // ==== SETUP ==== //
        Alamat nodeA = new Alamat("NodeA");
        Alamat nodeB = new Alamat("NodeB");

        // Log entries for testing
        Entry entry1 = new Entry(1, "set x=10");
        Entry entry2 = new Entry(1, "append x=1");
        List<Entry> entries = Arrays.asList(entry1, entry2);

        // ==== Test: VoteReq + VoteResp ====
        VoteReq voteReq = new VoteReq(2, nodeA, 5, 2);
        VoteResp voteResp = new VoteResp(2, true);

        System.out.println("=== Vote Request ===");
        System.out.println("Term: " + voteReq.getTerm());
        System.out.println("Candidate: " + voteReq.getCandidateId());
        System.out.println("LastLogIndex: " + voteReq.getLastLogIndex());
        System.out.println("LastLogTerm: " + voteReq.getLastLogTerm());

        System.out.println("=== Vote Response ===");
        System.out.println("Term: " + voteResp.getTerm());
        System.out.println("Vote Granted: " + voteResp.isVoteGranted());

        // ==== Test: NewEntryReq + NewEntryResp ====
        NewEntryReq newEntryReq = new NewEntryReq(
            2, nodeA, 4, 1, entries, 3, nodeB, nodeA
        );

        NewEntryResp newEntryResp = new NewEntryResp(2, true);

        System.out.println("=== AppendEntries (NewEntryReq) ===");
        System.out.println("From: " + newEntryReq.getFrom());
        System.out.println("To: " + newEntryReq.getTo());
        System.out.println("PrevLogIdx: " + newEntryReq.getPrevLogIdx());
        System.out.println("PrevLogTerm: " + newEntryReq.getPrevLogTerm());
        System.out.println("Entries: ");
        for (Entry e : newEntryReq.getEntries()) {
            System.out.println(" - [Term: " + e.getTerm() + ", Cmd: " + e.getCommand() + "]");
        }
        System.out.println("Leader Commit: " + newEntryReq.getLeaderCommit());

        System.out.println("=== AppendEntries Response ===");
        System.out.println("Term: " + newEntryResp.getTerm());
        System.out.println("Success: " + newEntryResp.isSuccess());

        // ==== Simulated RaftNode Logging (Basic Storage Logic) ====
        List<Entry> raftLog = new ArrayList<>();
        for (Entry e : entries) {
            raftLog.add(e);
        }

        System.out.println("=== Raft Log ===");
        for (int i = 0; i < raftLog.size(); i++) {
            System.out.println("Index " + i + ": " + raftLog.get(i).getCommand());
        }

        // ==== Simulated KV Store Commit ====
        Map<String, String> kvStore = new HashMap<>();
        for (Entry e : entries) {
            String cmd = e.getCommand();
            if (cmd.startsWith("set ")) {
                String[] parts = cmd.substring(4).split("=");
                kvStore.put(parts[0].trim(), parts[1].trim());
            } else if (cmd.startsWith("append ")) {
                String[] parts = cmd.substring(7).split("=");
                String key = parts[0].trim();
                String appendValue = parts[1].trim();
                kvStore.put(key, kvStore.getOrDefault(key, "") + appendValue);
            }
        }

        System.out.println("=== KV Store State ===");
        for (Map.Entry<String, String> entry : kvStore.entrySet()) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
    }
}
