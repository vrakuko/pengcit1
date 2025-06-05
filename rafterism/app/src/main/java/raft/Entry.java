package raft;

public class Entry {
    private int term;
    private String command;
    private String key;
    private String value;
    // private long timestamp;

    public Entry (){

    }

    public Entry(int term, String command, String key, String value) {
        this.term = term;
        this.command = command;
        this.key = key;
        this.value = value;
        // this.timestamp = System.currentTimeMillis();
    }

    // Getters
    public int getTerm() { return term; }
    public String getCommand() { return command; }
    public String getKey() { return key; }
    public String getValue() { return value; }
    // public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return String.format("LogEntry{term=%d, cmd=%s, key=%s, value=%s}",
                           term, command, key, value);
    }
}
