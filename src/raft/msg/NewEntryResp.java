package raft.msg;

import raft.entity.Alamat;

public class NewEntryResp {
    private int term;
    private boolean success;
    private Alamat from;
    private Alamat to;

    public NewEntryResp(int term, boolean success, Alamat from, Alamat to) {
        this.term = term;
        this.success = success;
        this.from = from;
        this.to = to;
    }

    public int getTerm() { return term; }
    public boolean isSuccess() { return success; }
    public Alamat getFrom() { return from; }
    public Alamat getTo() { return to; }
}
