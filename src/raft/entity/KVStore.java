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
                return del(key);
            case "get":
                return get(key);
            case "strlen":
                return String.valueOf(strLen(key));
            case "ping":
                return ping();
            default:
                return "ERROR: Unknown command";
        }
    }
}
