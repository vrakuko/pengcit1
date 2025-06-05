package raft.entity;

import java.util.HashMap;
import java.util.Map;

public class KVStore{

    
    private   Map<String, String> kvStore;
    // public CommandType cmd ; 
    
    public KVStore() {
        // this.cmd = null;
        this.kvStore = new HashMap<>();
    }


    
    public String ping() {
        return "PONG";
    }
    
    public String get(String key) {
        return kvStore.getOrDefault(key, "");
    }
    
    public void set(String key, String value) {
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
        String current = get(key);
        kvStore.put(key, current + value);
        System.out.println("OK");
    }

    public String executeCommand(String commandType, String key, String value) {
        switch (commandType.toLowerCase()) {
            case "set":
                set(key, value);
                return "OK";
            case "append":
                append(key, value);
                return "OK";
            case "del":
                return del(key); // Returns deleted value
            case "get":
                return get(key); // Returns value
            case "strlen":
                return String.valueOf(strLen(key)); // Returns length as string
            case "ping":
                return ping(); // Returns "PONG"
            default:
                return "ERROR: Unknown command";
        }
    }
}
