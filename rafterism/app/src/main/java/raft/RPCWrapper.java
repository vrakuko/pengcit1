// Buat file baru: RPCWrapper.java
package raft;

import java.util.concurrent.Callable;

import raft.proto.AppendEntriesRequest;
import raft.proto.AppendEntriesResponse;
import raft.proto.VoteRequest;
import raft.proto.VoteResponse;

public class RPCWrapper {
    
    public static VoteResponse callRequestVote(NodeAddr peer, VoteRequest request) {
        return callWithRetry(() -> RPCClient.sendVoteRequest(peer, request));
    }

    public static AppendEntriesResponse callAppendEntries(NodeAddr peer, AppendEntriesRequest request) {
        return callWithRetry(() -> RPCClient.sendAppendEntries(peer, request));
    }

    private static <V> V callWithRetry(Callable<V> rpcCall) {
        int maxRetries = 3;
        long delayMs= 250; 
        Exception lastException = null;

        for (int i = 0; i < maxRetries; i++) {
            try {
                return rpcCall.call();
            } catch (Exception e) {
                lastException = e;
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("RPC retry interrupted", interruptedException);
                }
            }
        }
        throw new RuntimeException("RPC call failed after " + maxRetries + " retries", lastException);
    }
}