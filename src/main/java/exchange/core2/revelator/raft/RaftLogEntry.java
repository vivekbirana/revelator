package exchange.core2.revelator.raft;

import java.nio.ByteBuffer;

/**
 * each entry contains command for state machine, and term when entry was received by leader
 */
public class RaftLogEntry<T extends RsmRequest> {

    // term when entry was received by leader
    public final int term;

    // command
    public final T cmd;

    public RaftLogEntry(int term, T cmd) {
        this.term = term;
        this.cmd = cmd;
    }

    public void serialize(ByteBuffer buffer) {
        buffer.putInt(term);
        cmd.serialize(buffer);
    }

    @Override
    public String toString() {
        return "RLE{" +
                "t" + term +
                " cmd=" + cmd +
                '}';
    }

    public static <T extends RsmRequest> RaftLogEntry<T> create(ByteBuffer buffer,
                                                                SerializableMessageFactory<T, ?> factory) {
        final int term = buffer.getInt();
        final T cmd = factory.createRequest(buffer);
        return new RaftLogEntry<T>(term, cmd);
    }
}
