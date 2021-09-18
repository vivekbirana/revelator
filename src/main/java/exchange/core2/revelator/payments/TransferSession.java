package exchange.core2.revelator.payments;

import exchange.core2.revelator.processors.pipelined.PipelinedFlowSession;

public final class TransferSession extends PipelinedFlowSession {

    public long accountSrc;
    public long accountDst;
    public boolean processSrc;
    public boolean processDst;
    public long revertAmount;

    //    public long feeAmount;
    //    public short currency;
}
