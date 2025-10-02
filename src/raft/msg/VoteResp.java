package raft.msg;

import raft.entity.Alamat;

public class VoteResp {
    private int term;
    private boolean voteGranted;
    private Alamat from;
    private Alamat to;

    public VoteResp(int term, boolean voteGranted, Alamat from, Alamat to) {
        this.term = term;
        this.voteGranted = voteGranted;
        this.from = from;
        this.to = to;
    }

    public int getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
    public Alamat getFrom() { return from; }
    public Alamat getTo() { return to; }
}
