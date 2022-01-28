package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

public interface SerializableMessageFactory<T extends RpcRequest> {

    T create(ByteBuffer buffer);

}
