package exchange.core2.revelator.raft;

import java.net.InetAddress;

public interface RpcHandler {

    RpcResponse handleNodeRequest(int nodeId, RpcRequest request);

    void handleNodeResponse(int nodeId, RpcResponse response, long correlationId);

    CustomCommandResponse handleClientRequest(InetAddress address, int port, long correlationId, CustomCommandRequest request);

}
