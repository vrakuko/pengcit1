package raft;

import java.util.concurrent.ExecutionException;

import com.google.gson.Gson;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;

public class ClientServer {
    private final RaftNode raftNode;
    private final Gson gson = new Gson();

    private static class CommandPayload { String command; String key; String value; }

    public ClientServer(RaftNode raftNode) { this.raftNode = raftNode; }

    public void start(int port) {
        port(port);
        get("/ping", (req, res) -> "PONG");
        post("/command", (req, res) -> {
            res.type("text/plain");
            CommandPayload payload = gson.fromJson(req.body(), CommandPayload.class);
            
            try {
                String result = raftNode.handleClientRequest(payload.command, payload.key, payload.value).get();
                if (!raftNode.isLeader() || result.startsWith("REDIRECT:")) {
                    NodeAddr leader = raftNode.getCurrentLeader();
                    if (leader != null) {
                        String leaderApiAddr = "localhost:" + (leader.getPort() + 1000);
                        res.status(200);
                        return "REDIRECT:" + leaderApiAddr;
                    } else {
                        res.status(503);
                        return "No leader available at the moment.";
                    }
                }
            
                res.status(200);
                return result;

            } catch (ExecutionException e) {
                res.status(500);
                return "Error executing command on leader: " + e.getCause().getMessage();
            }
        });

        get("/:key", (req, res) -> {
            res.type("text/plain");
            if (!raftNode.isLeader()) {
                NodeAddr leader = raftNode.getCurrentLeader();
                if (leader != null) {
                    String leaderApiAddr = "localhost:" + (leader.getPort() + 1000);
                    res.status(200);
                    return "REDIRECT:" + leaderApiAddr;
                } else {
                    res.status(503);
                    return "No leader available at the moment.";
                }
            }
            return raftNode.getKVStore().get(req.params(":key"));
        });
        
        get("/ping", (req, res) -> "PONG");
    }
}