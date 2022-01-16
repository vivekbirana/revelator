package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

public interface RpcResponse {


    int getMessageType();

    void serialize(ByteBuffer buffer);

    int RESPONSE_APPEND_ENTRIES = -1;
    int RESPONSE_VOTE = -2;

}
