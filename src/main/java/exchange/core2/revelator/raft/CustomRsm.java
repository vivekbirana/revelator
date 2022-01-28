package exchange.core2.revelator.raft;

import org.agrona.collections.Hashing;

public class CustomRsm implements ReplicatedStateMachine {

    int hash = 0;

    @Override
    public int apply(long value) {
        hash = Hashing.hash(hash ^ Hashing.hash(value));
        return hash;
    }

    @Override
    public int getState() {
        return hash;
    }
}
