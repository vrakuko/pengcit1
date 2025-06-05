package rafterism.rafter;

public class NewEntryResp {
    private int term;
    private boolean success;
    private NodeAddr from;
    private NodeAddr to;

    public NewEntryResp(int term, boolean success, NodeAddr from, NodeAddr to) {
        this.term = term;
        this.success = success;
        this.from = from;
        this.to = to;
    }

    public int getTerm() { return term; }
    public boolean isSuccess() { return success; }
    public NodeAddr getFrom() { return from; }
    public NodeAddr getTo() { return to; }
}
