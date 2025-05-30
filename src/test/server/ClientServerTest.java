package test.server;

import raft.entity.RaftNode;

public class ClientServerTest {
    private final RaftNode raftNode;
    
    public ClientServer(RaftNode raftNode) {
        this.raftNode = raftNode;
    }
    
    public void start(int port) {
        // TODO: Implement REST API server (Spring Boot or simple HTTP server)
        System.out.println("Client Server started on port " + port);
    }
    
    public void shutdown() {
        // TODO: Implement shutdown
    }
}

