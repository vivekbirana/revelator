package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.buffers.LocalResultsByteBuffer;

public final class ResponsesAggregator {


    private final LocalResultsByteBuffer resultsBuffer;
    private final IPaymentsResponseHandler responseHandler;

    private long lastAddr;

    public ResponsesAggregator(LocalResultsByteBuffer resultsBuffer,
                               IPaymentsResponseHandler responseHandler) {

        this.resultsBuffer = resultsBuffer;
        this.responseHandler = responseHandler;
    }

    public void handleMessage(final long addr,
                              final int msgSize,
                              final long timestamp,
                              final long correlationId,
                              final byte msgType) {

        // TODO mic from multiple buffers
        final long resultsCode = resultsBuffer.get(addr);

        this.lastAddr = addr;

        responseHandler.commandResult(
                timestamp,
                correlationId,
                (int) resultsCode,
                transferAccessor);


    }

    private final IPaymentsResponseHandler.ITransferAccessor transferAccessor = new IPaymentsResponseHandler.ITransferAccessor() {
        @Override
        public long getAccountFrom() {
            return Revelator.UNSAFE.getLong(lastAddr);
        }

        @Override
        public long getAccountTo() {
            return Revelator.UNSAFE.getLong(lastAddr + 8);
        }

        @Override
        public long getAmount() {
            return Revelator.UNSAFE.getLong(lastAddr + 16);
        }

        @Override
        public int getCurrency() {
            throw new UnsupportedOperationException();
        }
    };

}
