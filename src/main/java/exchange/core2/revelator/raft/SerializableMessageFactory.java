package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

public interface SerializableMessageFactory<T extends RsmRequest, S extends RsmResponse> {

    T createRequest(ByteBuffer buffer);

    S createResponse(ByteBuffer buffer);

    S emptyResponse();
}
