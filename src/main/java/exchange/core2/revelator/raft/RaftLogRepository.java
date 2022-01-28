package exchange.core2.revelator.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RaftLogRepository {


    private static final Logger log = LoggerFactory.getLogger(RaftLogRepository.class);

    private final List<RaftLogEntry> logEntries = new ArrayList<>(); // TODO change to persistent storage with long-index

    public RaftLogEntry getEntry(long index) {
        return logEntries.get((int) index - 1);
    }

    public long getLastLogIndex() {
        return logEntries.size(); // 0 = no records
    }

    public int getLastLogTerm() {
        if (logEntries.isEmpty()) {
            return 0; // return term 0 by default
        } else {
            return logEntries.get(logEntries.size() - 1).term;
        }
    }

    public long append(final RaftLogEntry logEntry) {
        logEntries.add(logEntry);
        return logEntries.size(); // starting from index=1
    }


    // size = 5
    // pos 0 1 2 3 4 5 6 7  <- array positions
    // idx 1 2 3 4 5        <- existing records

    // last        5
    // new           6 7 8

    // last    3
    // check     4 5
    // new           6 7 8

    // TODO unittest

    public void appendOrOverride(final List<RaftLogEntry> newEntries, long prevLogIndex) {

        for (int i = 0; i < newEntries.size(); i++) {
            final RaftLogEntry newEntry = newEntries.get(i);
            if ((prevLogIndex + i) < logEntries.size()) {
                final int pos = (int) prevLogIndex + i;
                final int existingTerm = logEntries.get(pos).term;

                // 3. If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow it

                if (newEntry.term != existingTerm) {
                    log.debug("Remove all records after index={}, because term is different: {} (old={})", pos + 1, newEntry.term, existingTerm);
                    int lastIdxToRemove = logEntries.size();
                    if (lastIdxToRemove > pos + 1) {
                        logEntries.subList(pos + 1, lastIdxToRemove).clear();
                    }
                }
            } else {
                logEntries.add(newEntry); // TODO inefficient, because normally records are simply appended as batch
            }
        }
    }

    // 1
    public List<RaftLogEntry> getEntriesStartingFrom(long nextIndex) {
        if (getLastLogIndex() < nextIndex) {
            return List.of();
        }

       return logEntries.subList((int) nextIndex - 1, logEntries.size());
    }
}
