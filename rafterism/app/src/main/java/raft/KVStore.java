package raft;

import java.util.HashMap;
import java.util.Map;

public class KVStore {
    private final Map<String, String> kvStore;

    public KVStore() {
        this.kvStore = new HashMap<>();
    }

    public String ping() {
        return "PONG";
    }

    public String get(String key) {
        return kvStore.getOrDefault(key, "");
    }

    public String set(String key, String value){
    kvStore.put(key, value);
    return "OK";
}

    public String append(String key, String value) {
        kvStore.compute(key, (k, v) -> (v == null ? "" : v) + value);
        return "OK";
    }

    public int strLen(String key) {
        return get(key).length();
    }

    public String del(String key) {
        String oldValue = kvStore.getOrDefault(key, "");
        kvStore.remove(key);
        return oldValue;
    }

    public String executeCommand(String commandType, String key, String value) {
        if (commandType == null) return "ERROR: CommandType is null";
        switch (commandType.toLowerCase()) {
            case "set":
                return set(key, value);
            case "append":
                return append(key, value);
            case "del":
                return del(key);
            case "get":
                return get(key);
            case "strlen":
                return String.valueOf(strLen(key));
            case "ping":
                return ping();
            default:
                System.err.println("[" + Thread.currentThread().getName() + "] Unknown command in KVStore: " + commandType);
                return "ERROR: Unknown command '" + commandType + "'";
        }
    }
}