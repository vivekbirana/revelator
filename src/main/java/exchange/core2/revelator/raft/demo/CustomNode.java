package exchange.core2.revelator.raft.demo;

import exchange.core2.revelator.raft.RaftNode;

public class CustomNode {

    public static void main(String[] args) {

        final int thisNodeId = Integer.parseInt(args[0]);

        final CustomRsm customRsm = new CustomRsm();

        new RaftNode<>(thisNodeId, customRsm, customRsm, customRsm);
    }

}
