package rafterism.rafter;

public class NewVoteResp {
    private int term;
    private boolean voteGranted;
    private NodeAddr from;
    private NodeAddr to;

    public NewVoteResp(int term, boolean voteGranted, NodeAddr from, NodeAddr to) {
        this.term = term;
        this.voteGranted = voteGranted;
        this.from = from;
        this.to = to;
    }

    public int getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
    public NodeAddr getFrom() { return from; }
    public NodeAddr getTo() { return to; }
}
