package exchange.core2.revelator.raft;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class RaftNode<T extends RaftMessage> {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    public static final int HEARTBEAT_TIMEOUT_MS = 2000 + (int) (Math.random() * 500);
    public static final int HEARTBEAT_LEADER_RATE_MS = 1000;
    public static final int ELECTION_TIMEOUT_MS = 3000;

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

    private final int currentNodeId;
    private final int[] otherNodes;

    private final RpcService rpcService;

    private Timer appendTimer;
    private Timer electionTimer;

    private ScheduledExecutorService heartbeatLeaderExecutor;

    public static void main(String[] args) {

        final int thisNodeId = Integer.parseInt(args[0]);

        new RaftNode(thisNodeId);
    }

    public RaftNode(int thisNodeId) {

        // localhost:3778, localhost:3779, localhost:3780
        final Map<Integer, String> remoteNodes = Map.of(
                0, "localhost:3778",
                1, "localhost:3779",
                2, "localhost:3780");

        this.currentNodeId = thisNodeId;

        this.otherNodes = remoteNodes.keySet().stream().mapToInt(x -> x).filter(nodeId -> nodeId != thisNodeId).toArray();


        final BiFunction<Integer, RpcRequest, RpcResponse> handler = (fromNodeId, req) -> {
            logger.debug("INCOMING REQ {} >>> {}", fromNodeId, req);

            if (req instanceof CmdRaftVoteRequest voteRequest) {

                synchronized (this) {
                    /* Receiver implementation:
                    1. Reply false if term < currentTerm (§5.1)
                    2. If votedFor is null or candidateId, and candidate’s log is at
                    least as up-to-date as receiver’s log, grant vote (5.2, 5.4) */

                    if (voteRequest.term < currentTerm) {
                        logger.debug("Reject vote for {} - term is old", fromNodeId);
                        return new CmdRaftVoteResponse(currentTerm, false);
                    }

                    if (voteRequest.term > currentTerm) {
                        logger.debug("received newer term {} with vote request", voteRequest.term);
                        currentTerm = voteRequest.term;
                        switchToFollower();
                    }

                    if (votedFor != -1 && votedFor != currentNodeId) {
                        logger.debug("Reject vote for {} - already voted for {}", fromNodeId, votedFor);
                        return new CmdRaftVoteResponse(currentTerm, false);
                    }

                    logger.debug("VOTE GRANTED for {}", fromNodeId);
                    votedFor = fromNodeId;

                    return new CmdRaftVoteResponse(currentTerm, true);

                }

            }
            if (req instanceof CmdRaftAppendEntries appendEntriesCmd) {

                synchronized (this) {

                    if (appendEntriesCmd.term < currentTerm) {
                        logger.debug("Ignoring leader with older term {} (current={}", appendEntriesCmd.term, currentTerm);
                        return new CmdRaftAppendEntriesResponse(currentTerm, false);
                    }

                    if (currentState == RaftNodeState.CANDIDATE) {
                        /* While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
                        If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
                        then the candidate recognizes the leader as legitimate and returns to follower state.
                        If the term in the RPC is smaller than the candidate’s current term,
                        then the candidate rejects the RPC and continues in candidate state. */

                        switchToFollower();

                        electionTimer.cancel();


                    } else {

                        // TODO add records

                        if (appendEntriesCmd.term > currentTerm) {
                            logger.info("Update term {}->{}", currentTerm, appendEntriesCmd.term);
                            currentTerm = appendEntriesCmd.term;
                        }

                        resetFollowerAppendTimer();

                    }
                }
            }

            return null;
        };

        final BiConsumer<Integer, RpcResponse> handlerResponses = (fromNodeId, resp) -> {

            logger.debug("INCOMING RESP {} >>> {}", fromNodeId, resp);

                /* A candidate wins an election if it receives votes from
                a majority of the servers in the full cluster for the same
                term. Each server will vote for at most one candidate in a
                given term, on a first-come-first-served basis
                (note: Section 5.4 adds an additional restriction on votes) */

            if (resp instanceof final CmdRaftVoteResponse voteResponse) {
                synchronized (this) {
                    if (currentState == RaftNodeState.CANDIDATE && voteResponse.voteGranted && voteResponse.term == currentTerm) {
                        switchToLeader();
                    }
                }
            }
        };

        // todo remove from constructor
        rpcService = new RpcService(remoteNodes, handler, handlerResponses, thisNodeId);

        logger.info("HEARTBEAT_TIMEOUT_MS={}", HEARTBEAT_TIMEOUT_MS);
        logger.info("ELECTION_TIMEOUT_MS={}", ELECTION_TIMEOUT_MS);

        logger.info("Starting node {} as follower...", thisNodeId);
        resetFollowerAppendTimer();
    }

    private void switchToFollower() {

        if (currentState == RaftNodeState.LEADER) {
            logger.debug("shutdown heartbeats");
            heartbeatLeaderExecutor.shutdown();
        }

        if (currentState == RaftNodeState.CANDIDATE && electionTimer != null) {
            logger.debug("cancelled elevtion timer");
            electionTimer.cancel();
        }

        if (currentState != RaftNodeState.FOLLOWER) {
            logger.debug("Switching to follower (reset votedFor, start append timer)");
            currentState = RaftNodeState.FOLLOWER;
            votedFor = -1;
            resetFollowerAppendTimer();
        }
    }

//    public void run(int port) {
//        try (final DatagramSocket serverSocket = new DatagramSocket(port)) {
//            final byte[] receiveData = new byte[8];
//            String sendString = "polo";
//            final byte[] sendData = sendString.getBytes(StandardCharsets.UTF_8);
//
//            logger.info("Listening on udp:{}:{}", InetAddress.getLocalHost().getHostAddress(), port);
//            final DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
//
//            while (true) {
//                serverSocket.receive(receivePacket);
//                String sentence = new String(receivePacket.getData(), 0,
//                        receivePacket.getLength());
//                logger.debug("RECEIVED: " + sentence);
//
//
//                DatagramPacket sendPacket = new DatagramPacket(
//                        sendData,
//                        sendData.length,
//                        receivePacket.getAddress(),
//                        receivePacket.getPort());
//
//                serverSocket.send(sendPacket);
//            }
//        } catch (IOException ex) {
//            System.out.println(ex);
//        }
//    }


//    /**
//     * Receiver implementation:<p>
//     * 1. Reply false if term < currentTerm (5.1)<p>
//     * 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (5.3)<p>
//     * 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (5.3)<p>
//     * 4. Append any new entries not already in the log<p>
//     * 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)<p>
//     */
//    public synchronized CmdRaftAppendEntriesResponse appendEntries(CmdRaftAppendEntries cmd) {
//
//        // 1. Reply false if term < currentTerm
//        // If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
//        if (cmd.term < currentTerm) {
//            logger.debug("term < currentTerm");
//            return new CmdRaftAppendEntriesResponse(currentTerm, false);
//        }
//
//        // If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, then the candidate
//        // recognizes the leader as legitimate and returns to follower state. I
//        checkTerm(cmd.term);
//
//        // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
//        if (cmd.prevLogIndex >= log.size() || log.get((int) cmd.prevLogIndex).term != cmd.prevLogTerm) {
//            logger.debug("log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm");
//            return new CmdRaftAppendEntriesResponse(currentTerm, false);
//        }
//
//        // TODO 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (5.3)
//
//        // Append any new entries not already in the log
//
//        resetFollowerAppendTimer();
//
//        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
//        if (cmd.leaderCommit > commitIndex) {
//            commitIndex = Math.min(cmd.leaderCommit, log.size() - 1);
//        }
//
//        return new CmdRaftAppendEntriesResponse(currentTerm, true);
//    }
//
//    /**
//     * Receiver implementation:
//     * 1. Reply false if term < currentTerm (5.1)
//     * 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (5.2, 5.4)
//     */
//    public synchronized CmdRaftVoteResponse handleVoteRequest(CmdRaftVoteRequest request) {
//
//        if (request.term < currentTerm) {
//            return new CmdRaftVoteResponse(currentTerm, false);
//        }
//
//        checkTerm(request.term);
//
//
//        final boolean notVotedYet = votedFor == -1 || votedFor == request.candidateId;
//        final boolean logIsUpToDate = request.lastLogIndex >= commitIndex; // TODO commitIndex or lastApplied?
//        if (notVotedYet && logIsUpToDate) {
//            // vote for candidate
//            votedFor = request.candidateId;
//            return new CmdRaftVoteResponse(currentTerm, true);
//        } else {
//            // reject voting
//            return new CmdRaftVoteResponse(currentTerm, false);
//        }
//
//    }
//
//
//    private void checkTerm(int term) {
//        // All servers: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (5.1)
//        if (term > currentTerm) {
//            logger.info("Newer term={} received from new leader, switching to FOLLOWER", term);
//            currentTerm = term;
//
//            if (currentState == RaftNodeState.LEADER) {
//                heartbeatLeaderExecutor.shutdown();
//            }
//
//            currentState = RaftNodeState.FOLLOWER;
//        }
//    }


    private void broadcast(RaftMessage message) {

        logger.debug("Sending: {}", message);

    }

    private synchronized void resetFollowerAppendTimer() {

        logger.debug("reset append timer");

        if (appendTimer != null) {
            appendTimer.cancel();
        }
        final TimerTask task = new TimerTask() {
            @Override
            public void run() {
                appendTimeout();
            }
        };

        appendTimer = new Timer();
        appendTimer.schedule(task, HEARTBEAT_TIMEOUT_MS);
    }

    /**
     * To begin an election, a follower increments its current
     * term and transitions to candidate state. It then votes for
     * itself and issues RequestVote RPCs in parallel to each of
     * the other servers in the cluster. A candidate continues in
     * this state until one of three things happens:
     * (a) it wins the election,
     * (b) another server establishes itself as leader, or
     * (c) a period of time goes by with no winner.
     */
    private synchronized void appendTimeout() {

        // TODO double-check last receiving time (and get rid of timers)

        if (currentState == RaftNodeState.LEADER) {
            heartbeatLeaderExecutor.shutdown();
        }

        currentState = RaftNodeState.CANDIDATE;

        // On conversion to candidate, start election:
        // - Increment currentTerm
        // - Vote for self
        // - Reset election timer
        // - Send RequestVote RPCs to all other servers
        currentTerm++;

        logger.info("heartbeat timeout - switching to CANDIDATE, term={}", currentTerm);

        votedFor = currentNodeId;


        final CmdRaftVoteRequest voteReq = new CmdRaftVoteRequest(
                currentTerm,
                currentNodeId,
                lastApplied,
                429384628); // TODO extract from log!

//        try {

        final CompletableFuture<RpcResponse> future0 = rpcService.callRpcSync(voteReq, otherNodes[0]);
        final CompletableFuture<RpcResponse> future1 = rpcService.callRpcSync(voteReq, otherNodes[1]);

        final TimerTask task = new TimerTask() {
            @Override
            public void run() {
                appendTimeout();
            }
        };

        electionTimer = new Timer();
        electionTimer.schedule(task, ELECTION_TIMEOUT_MS);
    }

    private void switchToLeader() {

        logger.info("Becoming a LEADER!");
        currentState = RaftNodeState.LEADER;
        if (electionTimer != null) {
            electionTimer.cancel();
        }
        if (appendTimer != null) {
            appendTimer.cancel();
        }

        heartbeatLeaderExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatLeaderExecutor.scheduleAtFixedRate(
                () -> {
                    logger.info("Sending heartbeats, term={}", currentTerm);
                    final CmdRaftAppendEntries heartBeatReq = new CmdRaftAppendEntries(
                            currentTerm,
                            currentNodeId,
                            lastApplied,
                            429384628,
                            List.of(),
                            commitIndex);
                    rpcService.callRpcSync(heartBeatReq, otherNodes[0]);
                    rpcService.callRpcSync(heartBeatReq, otherNodes[1]);
                },
                0,
                HEARTBEAT_LEADER_RATE_MS,
                TimeUnit.MILLISECONDS);

        logger.info("Leader initiated");

        // TODO init
        // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
//                private final long[] nextIndex = new long[3];

        // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)
//                private final long[] matchIndex = new long[3];
    }

    private synchronized void electionTimeout() {
        logger.info("election timeout - switching to FOLLOWER");
    }

    public enum RaftNodeState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
}