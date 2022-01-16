package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

public interface RpcRequest {

    int getMessageType();

    void serialize(ByteBuffer buffer);

    int REQUEST_APPEND_ENTRIES = 1;
    int REQUEST_VOTE = 2;
}
