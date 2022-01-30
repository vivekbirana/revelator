package exchange.core2.revelator.raft.demo;

import exchange.core2.revelator.raft.ReplicatedStateMachine;
import exchange.core2.revelator.raft.RsmMessageFactory;
import org.agrona.collections.Hashing;

import java.nio.ByteBuffer;

public class CustomRsm implements
        ReplicatedStateMachine<CustomRsmCommand, CustomRsmResponse>,
        RsmMessageFactory<CustomRsmCommand, CustomRsmResponse> {

    public static final CustomRsmResponse EMPTY_RSM_RESPONSE = new CustomRsmResponse(0);

    // state
    private int hash = 0;

    @Override
    public CustomRsmResponse applyCommand(CustomRsmCommand cmd) {
        hash = Hashing.hash(hash ^ Hashing.hash(cmd.data()));
        return new CustomRsmResponse(hash);
    }

    @Override
    public CustomRsmResponse applyQuery(CustomRsmCommand query) {
        // can not change anything
        return new CustomRsmResponse(hash);
    }

    @Override
    public CustomRsmResponse getState() {
        return new CustomRsmResponse(hash);
    }

    @Override
    public CustomRsmCommand createRequest(ByteBuffer buffer) {
        return CustomRsmCommand.create(buffer);
    }

    @Override
    public CustomRsmResponse createResponse(ByteBuffer buffer) {
        return CustomRsmResponse.create(buffer);
    }

    @Override
    public CustomRsmResponse emptyResponse() {
        return EMPTY_RSM_RESPONSE;
    }
}
