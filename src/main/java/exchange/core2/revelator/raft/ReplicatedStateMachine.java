package exchange.core2.revelator.raft;

public interface ReplicatedStateMachine<T extends RsmRequest, S extends RsmResponse> {

    S applyCommand(T command);

    // TODO query
    S applyQuery(T query);

    S getState();

}
