import java.util.HashMap;
import java.util.Map;

public class KVStore{
    public enum CommandType { PING, GET, SET, STRLEN, DEL, APPEND }
    
    private   Map<String, String> kvStore;
    // private CommandType cmd ; 
    
    public KVStore() {
        // this.cmd = null;
        this.kvStore = new HashMap<>();
    }
    // public Command(CommandType cmd, String ) {
    //     this.cmd = cmd;
    //     this.kvStore = new HashMap<>();
    // }
    public void executeCommand(CommandType type, String key, String value) {
        try {
            switch (type) {
                case PING : ping();
                case GET : get(key);
                case SET : set(key, value);
                case STRLEN : strLen(key);
                case DEL : del(key);
                case APPEND : append(key, value);
                default : System.out.println("ERROR: Invalid command");
            };
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
        }
    }
    
    private String ping() {
        return "PONG";
    }
    
    private String get(String key) {
        return kvStore.getOrDefault(key, "");
    }
    
    private void set(String key, String value) {
        // if (key == null || value == null) {
        //     return "ERROR: Key and value cannot be null";
        // }
        kvStore.put(key, value);
       System.out.println("OK");
    }
    
    private int strLen(String key) {
        return get(key).length();
    }
    
    private String del(String key) {
        String value = get(key);
        kvStore.remove(key);
        return value; 
    }
    
    private void append(String key, String value) {
        // if (key == null || value == null) {
        //     return "ERROR: Key and value cannot be null";
        // }
        String current = get(key);
        kvStore.put(key, current + value);
        System.out.println("OK");
    }
}