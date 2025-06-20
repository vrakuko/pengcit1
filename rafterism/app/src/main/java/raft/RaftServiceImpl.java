package raft;

import io.grpc.stub.StreamObserver;
import raft.proto.AppendEntriesRequest;
import raft.proto.AppendEntriesResponse;
import raft.proto.RaftServiceGrpc;
import raft.proto.VoteRequest;
import raft.proto.VoteResponse;

public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftNode raftNode;

    public RaftServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void requestVote(VoteRequest request, StreamObserver<VoteResponse> responseObserver) {
        VoteResponse response = raftNode.handleVoteRequest(request); 
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        AppendEntriesResponse response = raftNode.handleAppendEntries(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}