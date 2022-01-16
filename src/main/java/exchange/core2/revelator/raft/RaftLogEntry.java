package exchange.core2.revelator.raft;

/**
 * each entry contains command for state machine, and term when entry was received by leader
 */
public class RaftLogEntry {

    // term when entry was received by leader
    public final long term;
    public final String cmd;

    public RaftLogEntry(long term, String cmd) {
        this.term = term;
        this.cmd = cmd;
    }
}
