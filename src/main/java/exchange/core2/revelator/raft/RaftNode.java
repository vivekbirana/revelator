package exchange.core2.revelator.raft;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;

public class RaftNode<T extends RaftMessage> {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    public static final int HEARTBEAT_TIMEOUT_MS = 2000;

    public static final int CLUSTER_SIZE = 3;
    public static final int VOTES_REQUIRED = 2;


    /* **** Persistent state on all servers: (Updated on stable storage before responding to RPCs) */

    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    private int currentTerm = 0;

    // candidateId that received vote in current term (or -1 if none)
    private int votedFor = -1;

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private final List<RaftLogEntry> log = new ArrayList<>(); // TODO change to persistent storage with long-index


    /* **** Volatile state on all servers: */

    // index of the highest log entry known to be committed (initialized to 0, increases monotonically)
    private long commitIndex = 0;

    // index of the highest log entry applied to state machine (initialized to 0, increases monotonically)
    private long lastApplied = 0;

    private RaftNodeState currentState = RaftNodeState.FOLLOWER;

    /* **** Volatile state on leaders: (Reinitialized after election) */

    // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    private final long[] nextIndex = new long[3];

    // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    private final long[] matchIndex = new long[3];

    /* ********************************************* */

    private final Timer timer = new Timer("AppendTimer");


    public static void main(String[] args) {
        new RaftNode().run(3778);
    }

    public RaftNode(int thisNodeId) {
        logger.info("Starting node {} as follower...", thisNodeId);
        resetFollowerAppendTimer();
    }

    public void run(int port) {
        try (final DatagramSocket serverSocket = new DatagramSocket(port)) {
            final byte[] receiveData = new byte[8];
            String sendString = "polo";
            final byte[] sendData = sendString.getBytes(StandardCharsets.UTF_8);

            logger.info("Listening on udp:{}:{}", InetAddress.getLocalHost().getHostAddress(), port);
            final DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

            while (true) {
                serverSocket.receive(receivePacket);
                String sentence = new String(receivePacket.getData(), 0,
                        receivePacket.getLength());
                logger.debug("RECEIVED: " + sentence);


                DatagramPacket sendPacket = new DatagramPacket(
                        sendData,
                        sendData.length,
                        receivePacket.getAddress(),
                        receivePacket.getPort());

                serverSocket.send(sendPacket);
            }
        } catch (IOException ex) {
            System.out.println(ex);
        }
    }





    /**
     * Receiver implementation:<p>
     * 1. Reply false if term < currentTerm (5.1)<p>
     * 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (5.3)<p>
     * 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (5.3)<p>
     * 4. Append any new entries not already in the log<p>
     * 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)<p>
     */
    public synchronized CmdRaftAppendEntriesResponse appendEntries(CmdRaftAppendEntries cmd) {

        // 1. Reply false if term < currentTerm
        // If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
        if (cmd.term < currentTerm) {
            logger.debug("term < currentTerm");
            return new CmdRaftAppendEntriesResponse(currentTerm, false);
        }

        // If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, then the candidate
        // recognizes the leader as legitimate and returns to follower state. I
        checkTerm(cmd.term);

        // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if (cmd.prevLogIndex >= log.size() || log.get((int) cmd.prevLogIndex).term != cmd.prevLogTerm) {
            logger.debug("log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm");
            return new CmdRaftAppendEntriesResponse(currentTerm, false);
        }

        // TODO 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (5.3)

        // Append any new entries not already in the log

        resetFollowerAppendTimer();

        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (cmd.leaderCommit > commitIndex) {
            commitIndex = Math.min(cmd.leaderCommit, log.size() - 1);
        }

        return new CmdRaftAppendEntriesResponse(currentTerm, true);
    }

    /**
     * Receiver implementation:
     * 1. Reply false if term < currentTerm (5.1)
     * 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (5.2, 5.4)
     */
    public synchronized CmdRaftVoteResponse handleVoteRequest(CmdRaftVoteRequest request) {

        if (request.term < currentTerm) {
            return new CmdRaftVoteResponse(currentTerm, false);
        }

        checkTerm(request.term);


        final boolean notVotedYet = votedFor == -1 || votedFor == request.candidateId;
        final boolean logIsUpToDate = request.lastLogIndex >= commitIndex; // TODO commitIndex or lastApplied?
        if (notVotedYet && logIsUpToDate) {
            // vote for candidate
            votedFor = request.candidateId;
            return new CmdRaftVoteResponse(currentTerm, true);
        } else {
            // reject voting
            return new CmdRaftVoteResponse(currentTerm, false);
        }

    }



    private void checkTerm(int term) {
        // All servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
        if (term > currentTerm) {
            logger.info("Newer term={} received from new leader, switching to FOLLOWER", term);
            currentTerm = term;
            currentState = RaftNodeState.FOLLOWER;
        }
    }


    private void broadcast(RaftMessage message) {

        logger.debug("Sending: {}", message);

    }

    private void resetFollowerAppendTimer(){

        logger.debug("reset append timer");

        timer.cancel();

        final TimerTask task = new TimerTask() {
            @Override
            public void run() {
                appendTimeout();
            }
        };

        timer.schedule(task, HEARTBEAT_TIMEOUT_MS);
    }

    private synchronized void appendTimeout(){

        logger.info("heartbeat timeout - switching to CANDIDATE");
        currentState = RaftNodeState.CANDIDATE;

        // On conversion to candidate, start election:
        // - Increment currentTerm
        // - Vote for self
        // - Reset election timer
        // - Send RequestVote RPCs to all other servers
        currentTerm ++;



    }


    public enum RaftNodeState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
}