package src.main.java.com.objek;
public class VoteRequest {
    private final int term;
    private final Address candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;
    
    public VoteRequest(int term, Address candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
    
    public int getTerm() { return term; }
    public Address getCandidateId() { return candidateId; }
    public int getLastLogIndex() { return lastLogIndex; }
    public int getLastLogTerm() { return lastLogTerm; }
}

// File: VoteResponse.java
