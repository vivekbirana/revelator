package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

/**
 * Invoked by leader to replicate log entries (5.3); also used as heartbeat (5.2).
 */
public final class CmdRaftVoteRequest implements RpcRequest {

    public final int term; // candidate's term
    public final int candidateId; // candidate requesting vote

    public final long lastLogIndex; // index of candidate’s last log entry (5.4)
    public final int lastLogTerm; // term of candidate’s last log entry (5.4)

    public CmdRaftVoteRequest(int term,
                              int candidateId,
                              long lastLogIndex,
                              int lastLogTerm) {

        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public int getMessageType() {
        return 2;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.putInt(candidateId);
        buffer.putLong(lastLogIndex);
        buffer.putInt(lastLogTerm);
    }


    public static CmdRaftVoteRequest create(ByteBuffer buffer) {

        final int term = buffer.getInt();
        final int leaderId = buffer.getInt();
        final long prevLogIndex = buffer.getLong();
        final int prevLogTerm = buffer.getInt();

        return new CmdRaftVoteRequest(term, leaderId, prevLogIndex, prevLogTerm);
    }
}
