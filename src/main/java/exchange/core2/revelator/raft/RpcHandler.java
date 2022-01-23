package exchange.core2.revelator.raft;

public interface RpcHandler {

    RpcResponse handleRequest(int nodeId, RpcRequest request);

    void handleResponse(int nodeId, RpcResponse response);

}
