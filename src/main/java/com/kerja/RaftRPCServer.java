package src.main.java.com.kerja;
public class RaftRPCServer {
    private final RaftNode raftNode;
    
    public RaftRPCServer(RaftNode raftNode) {
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