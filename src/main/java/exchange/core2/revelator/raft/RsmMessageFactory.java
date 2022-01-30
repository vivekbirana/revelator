package exchange.core2.revelator.raft;

import exchange.core2.revelator.raft.messages.RsmRequest;
import exchange.core2.revelator.raft.messages.RsmResponse;

import java.nio.ByteBuffer;

public interface RsmMessageFactory<T extends RsmRequest, S extends RsmResponse> {

    T createRequest(ByteBuffer buffer);

    S createResponse(ByteBuffer buffer);

    S emptyResponse();
}
