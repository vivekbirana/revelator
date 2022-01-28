package exchange.core2.revelator.raft;

public interface ReplicatedStateMachine {

    // TODO switch to custom messages
    int apply(long value);

    int getState();

}
