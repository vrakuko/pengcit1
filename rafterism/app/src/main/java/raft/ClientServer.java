package raft;

import com.google.gson.Gson;

import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;

public class ClientServer {
    private final RaftNode raftNode;
    private final Gson gson = new Gson();

    private static class CommandPayload { String command; String key; String value; }

    public ClientServer(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    public void start(int port) {
        port(port);

        get("/log", (req, res) -> {
            res.type("application/json");
            return gson.toJson(raftNode.getLogForClient());
        });

        get("/ping", (req, res) -> "PONG");

        get("/:key", (req, res) -> {
            res.type("text/plain");
            if (!raftNode.isLeader()) {
                return handleRedirect(res);
            }
            return raftNode.getKVStore().get(req.params(":key"));
        });

        post("/command", (req, res) -> {
            res.type("text/plain");

            if (!raftNode.isLeader()) {
                return handleRedirect(res);
            }

            CommandPayload payload = gson.fromJson(req.body(), CommandPayload.class);
            try {
                String result = raftNode.handleClientRequest(payload.command, payload.key, payload.value).get();
                res.status(200);
                return result;
            } catch (Exception e) {
                res.status(500);
                String errorMessage = (e.getCause() != null) ? e.getCause().getMessage() : e.getMessage();
                return "Error executing command on leader: " + errorMessage;
            }
        });
    }

    private String handleRedirect(spark.Response res) {
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
}
