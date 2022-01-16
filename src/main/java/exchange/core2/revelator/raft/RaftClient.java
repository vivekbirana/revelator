package exchange.core2.revelator.raft;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;

public class RaftClient {

    private static final Logger log = LoggerFactory.getLogger(RaftClient.class);


    public static void main(String[] args) throws IOException, InterruptedException {
        final RaftClient raftClient = new RaftClient();

        while (true) {
            raftClient.sendEcho("TEST123");
            Thread.sleep(1000);
        }
    }

    private DatagramSocket socket;
    private InetAddress address;

    private byte[] buf;

    public RaftClient() throws SocketException, UnknownHostException {
        socket = new DatagramSocket();
        address = InetAddress.getByName("localhost");
    }

    public String sendEcho(String msg) throws IOException {
        buf = msg.getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, address, 3778);
        log.debug(">> {}", msg);
        socket.send(packet);
        packet = new DatagramPacket(buf, buf.length);
        socket.receive(packet);
        String received = new String(packet.getData(), 0, packet.getLength());
        log.debug("<< {}", received);
        return received;
    }

    public void close() {
        socket.close();
    }
}