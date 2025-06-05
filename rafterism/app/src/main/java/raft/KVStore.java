package raft;

import java.util.HashMap;
import java.util.Map;

public class KVStore {
    private final Map<String, String> kvStore;

    public KVStore() {
        this.kvStore = new HashMap<>();
    }

    public String ping() { // Mengembalikan String
        return "PONG";
    }

    public String get(String key) { // Mengembalikan String
        return kvStore.getOrDefault(key, "");
    }

    public String set(String key, String value) { // Mengembalikan String
        kvStore.put(key, value);
        // Berdasarkan spesifikasi, SET mengembalikan "OK"
        // Jika di spesifikasi tugas Anda, set hanya print "OK" dan tidak mengembalikan apa-apa,
        // maka executeCommand tidak bisa mengembalikan hasil dari set.
        return "OK";
    }

    public int strLen(String key) { // Mengembalikan int
        return get(key).length();
    }

    public String del(String key) { // Mengembalikan String
        String oldValue = kvStore.getOrDefault(key, "");
        kvStore.remove(key);
        return oldValue;
    }

    public String append(String key, String value) { // Mengembalikan String
        String current = get(key);
        kvStore.put(key, current + value);
        // Berdasarkan spesifikasi, APPEND mengembalikan "OK"
        return "OK";
    }

    // Metode ini HARUS mengembalikan String jika digunakan seperti di RaftNode
    public String executeCommand(String commandType, String key, String value) {
        if (commandType == null) return "ERROR: CommandType is null";
        switch (commandType.toLowerCase()) {
            case "set":
                return set(key, value); // set() mengembalikan String
            case "append":
                return append(key, value); // append() mengembalikan String
            case "del":
                return del(key); // del() mengembalikan String
            case "get":
                return get(key); // get() mengembalikan String
            case "strlen":
                return String.valueOf(strLen(key)); // strLen() mengembalikan int, dikonversi ke String
            case "ping":
                return ping(); // ping() mengembalikan String
            default:
                System.err.println("[" + Thread.currentThread().getName() + "] Unknown command in KVStore: " + commandType);
                return "ERROR: Unknown command '" + commandType + "'";
        }
    }
}