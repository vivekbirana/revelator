package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

// TODO support batching
public record CustomCommandRequest(long data) implements RpcRequest {

    @Override
    public int getMessageType() {
        return REQUEST_CUSTOM;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(data);
    }

    public static CustomCommandRequest create(ByteBuffer buffer) {
        final long data = buffer.getLong();
        return new CustomCommandRequest(data);
    }
}
