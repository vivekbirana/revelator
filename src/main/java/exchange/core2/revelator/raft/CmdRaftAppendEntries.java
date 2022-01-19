package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Invoked by leader to replicate log entries (5.3); also used as heartbeat (5.2).
 */
public final class CmdRaftAppendEntries implements RpcRequest {

    public final int term; // leader’s term
    public final int leaderId; // so follower can redirect clients

    public final long prevLogIndex; // index of log entry immediately preceding new ones
    public final int prevLogTerm;// term of prevLogIndex entry
    public final List<RaftMessage> entries; // log entries to store (empty for heartbeat; may send more than one for efficiency)
    public final long leaderCommit;// leader’s commitIndex

    public CmdRaftAppendEntries(int term,
                                int leaderId,
                                long prevLogIndex,
                                int prevLogTerm,
                                List<RaftMessage> entries,
                                long leaderCommit) {

        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    @Override
    public int getMessageType() {
        return 1;
    }

    @Override
    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.putInt(leaderId);
        buffer.putLong(prevLogIndex);
        buffer.putInt(prevLogTerm);
//        buffer.put
        buffer.putLong(leaderCommit);
    }

    public static CmdRaftAppendEntries create(ByteBuffer buffer){

        final int term = buffer.getInt();
        final int leaderId = buffer.getInt();
        final long prevLogIndex = buffer.getLong();
        final int prevLogTerm = buffer.getInt();
        // todo entries
        final long leaderCommit  = buffer.getLong();

        return  new CmdRaftAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, List.of(), leaderCommit);
    }

    @Override
    public String toString() {
        return "CmdRaftAppendEntries{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries=" + entries +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}
