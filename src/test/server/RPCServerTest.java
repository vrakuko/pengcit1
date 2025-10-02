package test.server;

import raft.entity.RaftNode;

public class RPCServerTest {
    private RaftNode raftNode;
    
    public RPCServer(RaftNode raftNode) {
        this.raftNode = raftNode;
    }
    
    public void start(int port) {
        // TODO: Implement gRPC server
        System.out.println("RPC Server started on port " + port);
    }
    
    public void shutdown() {
        // TODO: Implement shutdown
    }
}

