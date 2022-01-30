package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

public class CustomRsmCommand implements RsmRequest {

    final long data;

    public CustomRsmCommand(long data) {
        this.data = data;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(data);
    }

    public static CustomRsmCommand create(ByteBuffer buffer) {
        return new CustomRsmCommand(buffer.getLong());
    }
}
