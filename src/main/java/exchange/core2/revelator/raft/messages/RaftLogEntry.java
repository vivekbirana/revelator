package exchange.core2.revelator.raft.messages;

import exchange.core2.revelator.raft.RsmMessageFactory;

import java.nio.ByteBuffer;

/**
 * each entry contains command for state machine, and term when entry was received by leader
 */
public record RaftLogEntry<T extends RsmRequest>(int term, T cmd) {

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
                                                                RsmMessageFactory<T, ?> factory) {
        final int term = buffer.getInt();
        final T cmd = factory.createRequest(buffer);
        return new RaftLogEntry<T>(term, cmd);
    }
}
