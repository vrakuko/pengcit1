package src.main.java.com.objek;
public class VoteResponse {
    private final int term;
    private final boolean voteGranted;
    
    public VoteResponse(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
    
    public int getTerm() { return term; }
    public boolean isVoteGranted() { return voteGranted; }
}