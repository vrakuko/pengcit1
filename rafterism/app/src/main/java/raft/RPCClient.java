package raft;

import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import raft.proto.AppendEntriesRequest;
import raft.proto.AppendEntriesResponse;
import raft.proto.RaftServiceGrpc;
import raft.proto.VoteRequest;
import raft.proto.VoteResponse;

public class RPCClient {
    public static VoteResponse sendVoteRequest(NodeAddr target, VoteRequest request) throws StatusRuntimeException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(target.getHost(), target.getPort())
            .usePlaintext()
            .build();
        
        try {
            RaftServiceGrpc.RaftServiceBlockingStub stub = RaftServiceGrpc.newBlockingStub(channel);
            return stub.withDeadlineAfter(500, TimeUnit.MILLISECONDS).requestVote(request);
        } finally {
            channel.shutdownNow();
        }
    }

    public static AppendEntriesResponse sendAppendEntries(NodeAddr target, AppendEntriesRequest request) throws StatusRuntimeException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(target.getHost(), target.getPort())
            .usePlaintext()
            .build();
            
        try {
            RaftServiceGrpc.RaftServiceBlockingStub stub = RaftServiceGrpc.newBlockingStub(channel);

            long timeout = request.getEntriesCount() > 0 ? 500 : 100;
            return stub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).appendEntries(request);
        } finally {
            channel.shutdownNow();
        }
    }
}
