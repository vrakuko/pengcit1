package src.main.java.com.objek;
public class AppendEntriesResponse {
    private int term;
    private int lastIdxFrom;
    private boolean success;

    
    public AppendEntriesResponse(int term, boolean success) {
        this.term = term;
        this.success = success;
        from
    }
    
    public int getTerm() { return term; }
    public boolean isSuccess() { return success; }
}