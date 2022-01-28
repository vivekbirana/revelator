package exchange.core2.revelator.raft;

import org.agrona.PrintBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RpcService<T extends RpcRequest> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RpcService.class);

    private final AtomicLong correlationIdCounter = new AtomicLong(1L);
    private final Map<Long, CompletableFuture<RpcResponse>> futureMap = new ConcurrentHashMap<>();
    private final Map<Integer, RemoteUdpSocket> socketMap;
    private final int serverPort;
    private final int serverNodeId;
    private final RpcHandler handler;

    private DatagramSocket serverSocket;
//    private final SerializableMessageFactory<T> msgFactory;

    private volatile boolean active = true;

    public RpcService(Map<Integer, String> remoteNodes,
                      RpcHandler handler,
                      int serverNodeId) {

        final Map<Integer, RemoteUdpSocket> socketMap = new HashMap<>();
        remoteNodes.forEach((id, address) -> {

            try {
                final String[] split = address.split(":");

                final DatagramSocket socket = new DatagramSocket();
                final InetAddress host = InetAddress.getByName(split[0]);
                final int port = Integer.parseInt(split[1]);

                RemoteUdpSocket remoteUdpSocket = new RemoteUdpSocket(socket, host, port);

                socketMap.put(id, remoteUdpSocket);

            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });


        this.socketMap = socketMap;
        this.handler = handler;
        this.serverPort = socketMap.get(serverNodeId).port;
        this.serverNodeId = serverNodeId;

        try {
            this.serverSocket = new DatagramSocket(serverPort);
        } catch (final SocketException ex) {
            throw new RuntimeException(ex);
        }

        Thread t = new Thread(this::run);
        t.setDaemon(true);
        t.setName("ListenerUDP");
        t.start();


    }


    public void run() {

        try {
            logger.info("Listening at UDP {}:{}", InetAddress.getLocalHost().getHostAddress(), serverPort);
        } catch (UnknownHostException ex) {
            logger.warn("UnknownHostException: ", ex);
        }

        final byte[] receiveData = new byte[256]; // TODO set proper value

        final DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

        while (active) {

            try {
                serverSocket.receive(receivePacket);

                final ByteBuffer bb = ByteBuffer.wrap(receivePacket.getData(), 0, receivePacket.getLength());

                final int nodeId = bb.getInt();
                final int messageType = bb.getInt();
                final long correlationId = bb.getLong();

//                logger.debug("RECEIVED from {} mt={}: {}", nodeId, messageType, PrintBufferUtil.hexDump(receivePacket.getData(), 0, receivePacket.getLength()));

                final RpcMessage msg = createByType(messageType, bb);
                // TODO use msgFactory

                if (messageType < 0) {
                    // handler response

                    final CompletableFuture<RpcResponse> future = futureMap.remove(correlationId);
                    if (future != null) {
                        // complete future for future-based-calls
                        future.complete((RpcResponse) msg);
                    } else {
                        // handle response for full-async-calls
                        handler.handleNodeResponse(nodeId, (RpcResponse) msg, correlationId);
                    }

                } else {

                    if (msg instanceof CustomCommandRequest) {

                        final InetAddress address = receivePacket.getAddress();
                        final int port = receivePacket.getPort();

                        final CustomCommandResponse response = handler.handleClientRequest(address, port, correlationId, (CustomCommandRequest) msg);
                        if (response != null) {
                            respondToClient(address, port, correlationId, response);
                        }

                    } else {
                        // handle request
                        final RpcResponse response = handler.handleNodeRequest(nodeId, (RpcRequest) msg);
                        // send response
                        if (response != null) {
                            sendResponse(nodeId, correlationId, response);
                        }
                    }


                }

            } catch (final Exception ex) {
                String message = PrintBufferUtil.hexDump(receivePacket.getData(), 0, receivePacket.getLength());
                logger.error("Failed to process message from {}: {}", receivePacket.getAddress().getHostAddress(), message, ex);
            }
        }

        logger.info("UDP server shutdown");
        serverSocket.close();
    }

    static RpcMessage createByType(int messageType, ByteBuffer buffer) {
        return switch (messageType) {
            case RpcMessage.REQUEST_APPEND_ENTRIES -> CmdRaftAppendEntries.create(buffer);
            case RpcMessage.RESPONSE_APPEND_ENTRIES -> CmdRaftAppendEntriesResponse.create(buffer);
            case RpcMessage.REQUEST_VOTE -> CmdRaftVoteRequest.create(buffer);
            case RpcMessage.RESPONSE_VOTE -> CmdRaftVoteResponse.create(buffer);
            case RpcMessage.REQUEST_CUSTOM -> CustomCommandRequest.create(buffer);
            case RpcMessage.RESPONSE_CUSTOM -> CustomCommandResponse.create(buffer);
            default -> throw new IllegalArgumentException("Unknown messageType: " + messageType);
        };
    }

    private void sendResponse(int callerNodeId, long correlationId, RpcResponse response) {
        final byte[] array = new byte[64];
        ByteBuffer bb = ByteBuffer.wrap(array);

        bb.putInt(serverNodeId);
        bb.putInt(response.getMessageType());
        bb.putLong(correlationId);
        response.serialize(bb);

        send(callerNodeId, array, bb.position());
    }


    public long callRpcAsync(RpcRequest request, int toNodeId) {

        final long correlationId = correlationIdCounter.incrementAndGet();
        callRpc(request, toNodeId, correlationId);
        return correlationId;
    }

    public CompletableFuture<RpcResponse> callRpcSync(RpcRequest request, int toNodeId) {

        final long correlationId = correlationIdCounter.incrementAndGet();

        final CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        futureMap.put(correlationId, future);

        callRpc(request, toNodeId, correlationId);

        return future;
    }

    private void callRpc(RpcRequest request, int toNodeId, long correlationId) {

        final byte[] array = new byte[64];
        ByteBuffer bb = ByteBuffer.wrap(array);

        bb.putInt(serverNodeId);
        bb.putInt(request.getMessageType());
        bb.putLong(correlationId);

        request.serialize(bb);

        send(toNodeId, array, bb.position());
    }


    private void send(int nodeId, byte[] data, int length) {

        final RemoteUdpSocket remoteUdpSocket = socketMap.get(nodeId);
        final DatagramPacket packet = new DatagramPacket(data, length, remoteUdpSocket.address, remoteUdpSocket.port);

        try {
            remoteUdpSocket.socket.send(packet);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void respondToClient(InetAddress address, int port, long correlationId, RpcResponse response) {

        final byte[] array = new byte[64];
        ByteBuffer bb = ByteBuffer.wrap(array);

        // put only correlationId into the header
        bb.putLong(correlationId);
        response.serialize(bb);

        respondToClient(address, port, array, bb.position());
    }

    private void respondToClient(InetAddress address, int port, byte[] data, int length) {

        final DatagramPacket packet = new DatagramPacket(data, length, address, port);

        try {

            serverSocket.send(packet);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    @Override
    public void close() throws Exception {

        active = false;
    }


    public static final class RemoteUdpSocket {

        private final DatagramSocket socket; // TODO remove
        private final InetAddress address;
        private final int port;

        public RemoteUdpSocket(DatagramSocket socket, InetAddress address, int port) {
            this.socket = socket;
            this.address = address;
            this.port = port;
        }
    }


}
