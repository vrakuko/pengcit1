package raft;


public class NewVoteReq {
    private int term;
    private NodeAddr from;         // kandidat yg minta vote
    private NodeAddr to;           // follower yg dikirimi permintaan
    private int lastLogIndex;
    private int lastLogTerm;

    public NewVoteReq(int term, NodeAddr from, NodeAddr to, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.from = from;
        this.to = to;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() { return term; }
    public NodeAddr getFrom() { return from; }
    public NodeAddr getTo() { return to; }
    public int getLastLogIndex() { return lastLogIndex; }
    public int getLastLogTerm() { return lastLogTerm; }

    // Alias buat maintain backward compatibility / readability
    public NodeAddr getCandidateId() { return from; }
}
