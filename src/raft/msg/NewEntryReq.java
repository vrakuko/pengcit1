package raft.msg;


import java.util.List;

import raft.entity.Alamat;
import raft.entity.Entry;

public class NewEntryReq {

    private int term;
    private Alamat to;
    private Alamat from;
    private int prevLogIdx;
    private int prevLogTerm;
    private List<Entry> entries;
    private int leaderCommit;

    public NewEntryReq(int term, Alamat leaderId, int prevLogIndex,
                                int prevLogTerm, List<Entry> entries, int leaderCommit, Alamat to, Alamat from) {
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

    public Alamat getTo() {
        return to;
    }

    public Alamat getFrom() {
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

    public void setTo(Alamat to) {
        this.to = to;
    }

    public void setFrom(Alamat from) {
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

