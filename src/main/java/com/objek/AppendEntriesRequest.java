package src.main.java.com.objek;

import java.util.List;

public class AppendEntriesRequest {

    private int term;
    private Address to;
    private Address from;
    private int prevLogIdx;
    private int prevLogTerm;
    private List<LogEntry> entries;
    private int leaderCommit;

    public AppendEntriesRequest(int term, Address leaderId, int prevLogIndex,
                                int prevLogTerm, List<LogEntry> entries, int leaderCommit, Address to, Address from) {
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

    public Address getTo() {
        return to;
    }

    public Address getFrom() {
        return from;
    }

    public int getPrevLogIdx() {
        return prevLogIdx;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    // Setters
    public void setTerm(int term) {
        this.term = term;
    }

    public void setTo(Address to) {
        this.to = to;
    }

    public void setFrom(Address from) {
        this.from = from;
    }

    public void setPrevLogIdx(int prevLogIdx) {
        this.prevLogIdx = prevLogIdx;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public void setEntries(List<LogEntry> entries) {
        this.entries = entries;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }
}
