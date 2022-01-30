package exchange.core2.revelator.raft;

import exchange.core2.revelator.raft.messages.RaftLogEntry;
import exchange.core2.revelator.raft.messages.RsmRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RaftLogRepository<T extends RsmRequest> {


    private static final Logger log = LoggerFactory.getLogger(RaftLogRepository.class);

    private final List<RaftLogEntry<T>> logEntries = new ArrayList<>(); // TODO change to persistent storage with long-index

    public RaftLogEntry<T> getEntry(long index) {
        return logEntries.get((int) index - 1);
    }

    public Optional<RaftLogEntry<T>> getEntryOpt(long index) {
        if (index < 1 || index > logEntries.size()) {
            return Optional.empty();
        }

        return Optional.of(logEntries.get((int) index - 1));
    }

    public long lastEntryInTerm(long indexAfter, long indexBeforeIncl, int term) {

        int idx = (int) indexAfter;

        for (int i = (int) indexAfter + 1; i <= indexBeforeIncl; i++) {
            log.debug("i={}", i);
            if (logEntries.get(i - 1).term() == term) {
                idx = i;
            }
        }
        return idx;
    }


    public long getLastLogIndex() {
        return logEntries.size(); // 0 = no records
    }

    public int getLastLogTerm() {
        if (logEntries.isEmpty()) {
            return 0; // return term 0 by default
        } else {
            return logEntries.get(logEntries.size() - 1).term();
        }
    }

    public long append(final RaftLogEntry<T> logEntry) {
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

    public void appendOrOverride(final List<RaftLogEntry<T>> newEntries, long prevLogIndex) {

        log.debug("appendOrOverride(newEntries={} , prevLogIndex={}", newEntries, prevLogIndex);

        for (int i = 0; i < newEntries.size(); i++) {
            final RaftLogEntry<T> newEntry = newEntries.get(i);

            if ((prevLogIndex + i) < logEntries.size()) {


                final int pos = (int) prevLogIndex + i;
                final int existingTerm = logEntries.get(pos).term();

                log.debug("Validating older record with index={}: existingTerm={} newEntry.term={}", pos + 1, existingTerm, newEntry.term());

                // 3. If an existing entry conflicts with a new one (same index but different terms),
                // delete the existing entry and all that follow it

                if (newEntry.term() != existingTerm) {
                    log.debug("Remove all records after index={}, because term is different: {} (old={})", pos + 1, newEntry.term(), existingTerm);
                    int lastIdxToRemove = logEntries.size();
                    if (lastIdxToRemove > pos + 1) {
                        logEntries.subList(pos + 1, lastIdxToRemove).clear();
                    }
                }
            } else {
                log.debug("appendOrOverride - added {}", newEntry);
                logEntries.add(newEntry); // TODO inefficient, because normally records are simply appended as batch
            }
        }
    }

    // 1
    public List<RaftLogEntry<T>> getEntriesStartingFrom(long nextIndex) {
        if (getLastLogIndex() < nextIndex) {
            return List.of();
        }

        log.debug("getEntriesStartingFrom({}): logEntries: {}", nextIndex, logEntries);

        return new ArrayList<>(logEntries.subList((int) nextIndex - 1, logEntries.size()));
    }
}
