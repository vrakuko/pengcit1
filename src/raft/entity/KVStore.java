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
}
