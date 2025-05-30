package raft.msg;

import raft.entity.Alamat;

public class VoteReq {
    private int term;
    private Alamat from;         // kandidat yg minta vote
    private Alamat to;           // follower yg dikirimi permintaan
    private int lastLogIndex;
    private int lastLogTerm;

    public VoteReq(int term, Alamat from, Alamat to, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.from = from;
        this.to = to;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() { return term; }
    public Alamat getFrom() { return from; }
    public Alamat getTo() { return to; }
    public int getLastLogIndex() { return lastLogIndex; }
    public int getLastLogTerm() { return lastLogTerm; }

    // Alias buat maintain backward compatibility / readability
    public Alamat getCandidateId() { return from; }
}
