package raft.entity;

public class Entry {
    private int term;
    private int index;
    private String command;
    private String key;
    private String value;
    private long timestamp;

    public Entry() {
        this.timestamp = System.currentTimeMillis();
    }
    
    public Entry(int term, int index, String command, String key, String value) {
        this.term = term;
        this.index = index;
        this.command = command;
        this.key = key;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Constructor untuk command tanpa key/value (seperti ping)
    public Entry(int term, int index, String command) {
        this(term, index, command, null, null);
    }
    
    // Getters
    public int getTerm() { return term; }
    public int getIndex() { return index; }
    public String getCommand() { return command; }
    public String getKey() { return key; }
    public String getValue() { return value; }
    public long getTimestamp() { return timestamp; }
    
    // Setters
    public void setTerm(int term) { this.term = term; }
    public void setIndex(int index) { this.index = index; }
    public void setCommand(String command) { this.command = command; }
    public void setKey(String key) { this.key = key; }
    public void setValue(String value) { this.value = value; }
    
    public boolean isValid() {
        if (command == null) return false;
        
        String cmd = command.toLowerCase();
        switch (cmd) {
            case "ping":
                return true;
            case "get":
            case "strln":
            case "del":
                return key != null;
            case "set":
            case "append":
                return key != null && value != null;
            default:
                return false;
        }
    }
    
    @Override
    public String toString() {
        return String.format("Entry{term=%d, index=%d, cmd=%s, key=%s, value=%s}", 
                           term, index, command, key, value);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Entry entry = (Entry) obj;
        return term == entry.term && index == entry.index;
    }
    
    @Override
    public int hashCode() {
        return java.util.Objects.hash(term, index);
    }
}