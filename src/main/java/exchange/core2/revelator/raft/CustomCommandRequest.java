package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

// TODO support batching !!

public record CustomCommandRequest<T extends RsmRequest>(T rsmRequest) implements RpcRequest {

    @Override
    public int getMessageType() {
        return REQUEST_CUSTOM;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        rsmRequest.serialize(buffer);
    }

    public static <T extends RsmRequest> CustomCommandRequest<T> create(ByteBuffer buffer, SerializableMessageFactory<T, ?> factory) {

        return new CustomCommandRequest<>(factory.createRequest(buffer));
    }
}
