package test.msg;
import raft.entity.Alamat;

public class VoteReqTest {
    private final int term;
    private final Alamat candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;
    
    public VoteReq(int term, Alamat candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
    
    public int getTerm() { return term; }
    public Alamat getCandidateId() { return candidateId; }
    public int getLastLogIndex() { return lastLogIndex; }
    public int getLastLogTerm() { return lastLogTerm; }
}

// File: VoteResponse.java
