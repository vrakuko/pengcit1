package raft.entity;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KVStore {
    private final ConcurrentMap<String, String> kvStore;
    
    public KVStore() {
        this.kvStore = new ConcurrentHashMap<>();
    }
    
    public String ping() {
        return "PONG";
    }
    
    public String get(String key) {
        return kvStore.getOrDefault(key,"");
    }
    
    public String set(String key, String value){
        kvStore.put(key, value);
        return "OK";
    }
    
    public int strLen(String key) {
        return get(key).length();
    }
    
    public String del(String key) {
        String value = kvStore.remove(key);
        return value != null ? value : "";
    }
    
    public String append(String key, String value) {
        String current = get(key);
        kvStore.put(key, current + value);
        return "OK";
    }
    
    public String executeCommand(Entry entry) {
        if (!entry.isValid()) {
            throw new IllegalArgumentException("Invalid command: " + entry.getCommand());
        }
        
        String command = entry.getCommand().toLowerCase();
        switch (command) {
            case "ping":
                return ping();
            case "get":
                return "\"" + get(entry.getKey()) + "\"";
            case "set":
                return set(entry.getKey(), entry.getValue());
            case "strln":
                return String.valueOf(strLen(entry.getKey()));
            case "del":
                String deleted = del(entry.getKey());
                return "\"" + deleted + "\"";
            case "append":
                return append(entry.getKey(), entry.getValue());
            default:
                throw new IllegalArgumentException("Unknown command: " + command);
        }
    }
    
    public int size() {
        return kvStore.size();
    }
    
    public void clear() {
        kvStore.clear();
    }
    
    @Override
    public String toString() {
        return "KVStore{size=" + kvStore.size() + "}";
    }
}