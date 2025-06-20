// App.java
package raft;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors; 

public class App {
    public static void main(String[] args) throws IOException, InterruptedException {
        try {
            System.out.println("Waiting 5 seconds for the cluster network to stabilize..."); // delay untuk startup
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Startup delay was interrupted. Exiting.");
            return;
        }
        if (args.length < 2) {
            System.out.println("Usage: java -jar app.jar <node-id> <cluster-string>");
            System.out.println("Example: java -jar app.jar 0 localhost:8001,localhost:8002,localhost:8003");
            return;
        }

        int nodeId = Integer.parseInt(args[0]);
        String[] clusterAddrs = args[1].split(",");
        
        NodeAddr selfAddr = parseAddr(clusterAddrs[nodeId]);
        List<NodeAddr> cluster = Arrays.stream(clusterAddrs).map(App::parseAddr).collect(Collectors.toList());

        System.out.println("Starting Node " + selfAddr + " as part of cluster: " + cluster);

        // inisialisasi RaftNode
        RaftNode raftNode = new RaftNode(selfAddr, cluster);

        // server
        RPCServer rpcServer = new RPCServer(raftNode);
        rpcServer.start();

        // client
        int clientPort = selfAddr.getPort() + 1000; 
        ClientServer clientServer = new ClientServer(raftNode);
        clientServer.start(clientPort);

        // block sampai server mati
        rpcServer.blockUntilShutdown();
    }

    private static NodeAddr parseAddr(String addrStr) {
        String[] parts = addrStr.split(":");
        return new NodeAddr(parts[0], Integer.parseInt(parts[1]));
    }
}