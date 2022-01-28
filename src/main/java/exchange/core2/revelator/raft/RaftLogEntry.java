package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

/**
 * each entry contains command for state machine, and term when entry was received by leader
 */
public class RaftLogEntry {

    // term when entry was received by leader
    public final int term;

    // command
    public final long cmd;

    public RaftLogEntry(int term, long cmd) {
        this.term = term;
        this.cmd = cmd;
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        buffer.putLong(cmd);
    }

    public static RaftLogEntry create(ByteBuffer buffer) {
        final int term = buffer.getInt();
        final long cmd = buffer.getLong();
        return new RaftLogEntry(term, cmd);
    }
}
