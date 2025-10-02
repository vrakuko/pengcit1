package test.entity;

import raft.entity.Entry;

public class EntryTest {
    public static void main(String[] args) {
        Entry entry = new Entry(1, "set", "user", "varaz");

        System.out.println("Term: " + entry.getTerm());               // 1
        System.out.println("Command: " + entry.getCommand());         // set
        System.out.println("Key: " + entry.getKey());                 // user
        System.out.println("Value: " + entry.getValue());             // varaz
        System.out.println("String: " + entry.toString());            // toString result
    }
}
