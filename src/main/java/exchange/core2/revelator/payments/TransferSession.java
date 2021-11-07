package exchange.core2.revelator.payments;

import exchange.core2.revelator.processors.pipelined.PipelinedFlowSession;

public final class TransferSession extends PipelinedFlowSession {

    public long accountSrc;
    public long accountDst;
    public boolean processSrc;
    public boolean processDst;
    public long revertAmount;

    public long msgAmount;
    public TransferType transferType;

    //    public long feeAmount;
    //    public short currency;


    @Override
    public String toString() {
        return "TransferSession{" +
                "accountSrc=" + accountSrc +
                ", accountDst=" + accountDst +
                ", processSrc=" + processSrc +
                ", processDst=" + processDst +
                ", revertAmount=" + revertAmount +
                ", msgAmount=" + msgAmount +
                ", transferType=" + transferType +
                ", globalOffset=" + globalOffset +
                ", bufferIndex=" + bufferIndex +
                ", payloadSize=" + payloadSize +
                ", messageType=" + messageType +
                ", timestamp=" + timestamp +
                ", correlationId=" + correlationId +
                ", userCookie=" + userCookie +
                '}';
    }
}
