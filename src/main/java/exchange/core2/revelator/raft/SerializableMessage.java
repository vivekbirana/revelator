package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

public interface SerializableMessage {

    void serialize(ByteBuffer buffer);

}
