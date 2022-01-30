package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

public class CustomRsmResponse implements RsmResponse {

    final int hash;

    public CustomRsmResponse(int hash) {
        this.hash = hash;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(hash);
    }

    public static CustomRsmResponse create(ByteBuffer buffer) {
        return new CustomRsmResponse(buffer.getInt());
    }
}
