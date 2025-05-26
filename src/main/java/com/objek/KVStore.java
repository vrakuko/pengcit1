package src.main.java.com.objek;
import java.util.HashMap;
import java.util.Map;

public class KVStore{

    
    private   Map<String, String> kvStore;
    // public CommandType cmd ; 
    
    public KVStore() {
        // this.cmd = null;
        this.kvStore = new HashMap<>();
    }
    // public Command(CommandType cmd, String ) {
    //     this.cmd = cmd;
    //     this.kvStore = new HashMap<>();
    // }

    
    public String ping() {
        return "PONG";
    }
    
    public String get(String key) {
        return kvStore.getOrDefault(key, "");
    }
    
    public void set(String key, String value) {
        // if (key == null || value == null) {
        //     return "ERROR: Key and value cannot be null";
        // }
        kvStore.put(key, value);
       System.out.println("OK");
    }
    
    public int strLen(String key) {
        return get(key).length();
    }
    
    public String del(String key) {
        String value = get(key);
        kvStore.remove(key);
        return value; 
    }
    
    public void append(String key, String value) {
        // if (key == null || value == null) {
        //     return "ERROR: Key and value cannot be null";
        // }
        String current = get(key);
        kvStore.put(key, current + value);
        System.out.println("OK");
    }
}