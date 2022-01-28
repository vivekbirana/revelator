package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Invoked by leader to replicate log entries (5.3); also used as heartbeat (5.2).
 */
public record CmdRaftAppendEntries(int term,
                                   int leaderId,
                                   long prevLogIndex,
                                   int prevLogTerm,
                                   List<RaftLogEntry> entries,
                                   long leaderCommit) implements RpcRequest {

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
        buffer.putInt(entries.size());
        entries.forEach(entry -> entry.serialize(buffer));
        buffer.putLong(leaderCommit);
    }

    public static CmdRaftAppendEntries create(ByteBuffer buffer) {

        final int term = buffer.getInt();
        final int leaderId = buffer.getInt();
        final long prevLogIndex = buffer.getLong();
        final int prevLogTerm = buffer.getInt();
        final int numEntries = buffer.getInt();

        final List<RaftLogEntry> entries = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            entries.add(RaftLogEntry.create(buffer));
        }

        final long leaderCommit = buffer.getLong();

        return new CmdRaftAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
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
