// Buat file baru di paket 'raft' (atau 'raft.server')
package raft;

import java.io.IOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class RPCServer {
    private final Server server;
    private final RaftNode raftNode;

    public RPCServer(RaftNode raftNode) {
        this.raftNode = raftNode;
        this.server = ServerBuilder.forPort(raftNode.getNodeAdr().getPort())
            .addService(new RaftServiceImpl(raftNode))
            .build();
    }

    public void start() throws IOException {
        server.start();
        System.out.println("[" + raftNode.getNodeAdr() + "] RPC Server started on port " + server.getPort());
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}