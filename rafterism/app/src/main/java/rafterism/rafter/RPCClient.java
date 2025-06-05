package rafterism.rafter;

public class RPCClient {
    private final NodeAddr targetNode;

    public RPCClient(NodeAddr targetNode) {
        this.targetNode = targetNode;
    }

    public NewVoteResp requestVote(NewVoteReq request) throws Exception {
        // This is a dummy implementation. In a real gRPC setup,
        // you would use a gRPC stub here.
        System.out.println("[RPCClient] Simulating RequestVote to " + targetNode + " from " + request.getFrom() + " for Term " + request.getTerm());
        // Simulate a successful vote for now, or a denial if the term is higher
        if (request.getTerm() == 1) { // Simple condition for dummy
             return new NewVoteResp(request.getTerm(), true, targetNode, request.getFrom());
        } else {
             return new NewVoteResp(request.getTerm(), false, targetNode, request.getFrom());
        }
        // In a real scenario:
        // YourGrpcServiceStub stub = YourGrpcServiceStub.newBlockingStub(channel);
        // return stub.requestVote(request);
    }

    public NewEntryResp appendEntries(NewEntryReq request) throws Exception {
        // This is a dummy implementation for heartbeats.
        System.out.println("[RPCClient] Simulating AppendEntries (Heartbeat: " + request.getEntries().isEmpty() + ") to " + targetNode + " from " + request.getFrom() + " for Term " + request.getTerm());
        // Simulate success for now
        return new NewEntryResp(request.getTerm(), true, targetNode, request.getFrom());
        // In a real scenario:
        // YourGrpcServiceStub stub = YourGrpcServiceStub.newBlockingStub(channel);
        // return stub.appendEntries(request);
    }
}
