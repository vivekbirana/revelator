package exchange.core2.revelator.raft.demo;

import exchange.core2.revelator.raft.messages.RsmRequest;

import java.nio.ByteBuffer;

public record CustomRsmCommand(long data) implements RsmRequest {

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(data);
    }

    @Override
    public String toString() {
        return "CRC{" +
                "data=" + data +
                '}';
    }

    public static CustomRsmCommand create(ByteBuffer buffer) {
        return new CustomRsmCommand(buffer.getLong());
    }
}
