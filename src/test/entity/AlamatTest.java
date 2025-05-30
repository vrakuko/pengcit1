package test.entity;

import raft.entity.Alamat;

public class AlamatTest {
    public static void main(String[] args) {
        Alamat a1 = new Alamat("localhost", 8080);
        Alamat a2 = new Alamat("localhost", 8080);
        Alamat a3 = new Alamat("127.0.0.1", 9090);

        System.out.println("Testing toString:");
        System.out.println(a1); // localhost:8080

        System.out.println("\nTesting equality:");
        System.out.println("a1 == a3? " + a1.equals(a3)); // false, beda reference

        System.out.println("\nTesting getters:");
        System.out.println("a1 host: " + a1.getHost());
        System.out.println("a1 port: " + a1.getPort());

        System.out.println("\nTesting setters:");
        a3.setHost("192.168.0.1");
        a3.setPort(7070);
        System.out.println("a3 modified: " + a3.toString());
    }
}
