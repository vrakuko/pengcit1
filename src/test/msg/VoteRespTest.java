package test.msg;

public class VoteRespTest {
    private final int term;
    private final boolean voteGranted;
    
    public VoteResp(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
    
    public int getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
}
