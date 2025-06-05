package raft;

import java.util.List;

public class NewEntryReq {
    private int term;
    private NodeAddr to;
    private NodeAddr from;
    private int prevLogIdx;
    private int prevLogTerm;
    private List<Entry> entries;
    private int leaderCommit;

    public NewEntryReq(int term, NodeAddr leaderId, int prevLogIndex,
                                int prevLogTerm, List<Entry> entries, int leaderCommit, NodeAddr to, NodeAddr from) {
        this.term = term;
        this.to = to;
        this.from = from;
        this.prevLogIdx = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    // Getters
    public int getTerm() {
        return term;
    }

    public NodeAddr getTo() {
        return to;
    }

    public NodeAddr getFrom() {
        return from;
    }

    public int getPrevLogIdx() {
        return prevLogIdx;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    // Setters
    public void setTerm(int term) {
        this.term = term;
    }

    public void setTo(NodeAddr to) {
        this.to = to;
    }

    public void setFrom(NodeAddr from) {
        this.from = from;
    }

    public void setPrevLogIdx(int prevLogIdx) {
        this.prevLogIdx = prevLogIdx;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public void setEntries(List<Entry> entries) {
        this.entries = entries;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }
}
