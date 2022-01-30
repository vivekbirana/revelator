package exchange.core2.revelator.raft;

import exchange.core2.revelator.raft.messages.RsmRequest;
import exchange.core2.revelator.raft.messages.RsmResponse;

public interface ReplicatedStateMachine<T extends RsmRequest, S extends RsmResponse> {

    S applyCommand(T command);

    // TODO query
    S applyQuery(T query);

    S getState();

}
