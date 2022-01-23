package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

public record CustomCommandResponse(long hash, int leaderNodeId, boolean success) implements RpcResponse {

    @Override
    public int getMessageType() {
        return RESPONSE_CUSTOM;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(hash);
        buffer.putInt(leaderNodeId);
        buffer.put(success ? (byte) 1 : (byte) 0);
    }

    public static CustomCommandResponse create(ByteBuffer buffer) {

        final long hash = buffer.getLong();
        final int leaderNodeId = buffer.getInt();
        final boolean success = buffer.get() == 1;

        return new CustomCommandResponse(hash, leaderNodeId, success);
    }

}
