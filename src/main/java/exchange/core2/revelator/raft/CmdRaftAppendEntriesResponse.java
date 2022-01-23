package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

/**
 * Invoked by leader to replicate log entries (5.3); also used as heartbeat (5.2).
 */
public record CmdRaftAppendEntriesResponse(int term, boolean success) implements RpcResponse {

    @Override
    public int getMessageType() {
        return RpcResponse.RESPONSE_APPEND_ENTRIES;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.put(success ? (byte) 1 : (byte) 0);
    }

    public static CmdRaftAppendEntriesResponse create(ByteBuffer bb) {
        final int term = bb.getInt();
        final boolean success = bb.get() == 1;
        return new CmdRaftAppendEntriesResponse(term, success);
    }

    @Override
    public String toString() {
        return "CmdRaftAppendEntriesResponse{" +
                "term=" + term +
                ", success=" + success +
                '}';
    }
}
