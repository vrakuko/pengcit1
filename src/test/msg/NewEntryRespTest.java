package test.msg;

public class NewEntryRespTest {
    private int term;
    private boolean success;

    public NewEntryResp(int term, boolean success) {
        this.term = term;
        this.success = success;
        
    }

    public int getTerm() { return term; }
    public boolean isSuccess() { return success; }
}
