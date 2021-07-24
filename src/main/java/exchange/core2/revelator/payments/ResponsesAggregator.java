package exchange.core2.revelator.payments;

import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;

public final class ResponsesAggregator implements SimpleMessageHandler {


    private final LocalResultsByteBuffer resultsBuffer;
    private final IPaymentsResponseHandler responseHandler;

    private int lastAddr;
    private long[] lastBuf;

    public ResponsesAggregator(LocalResultsByteBuffer resultsBuffer,
                               IPaymentsResponseHandler responseHandler) {

        this.resultsBuffer = resultsBuffer;
        this.responseHandler = responseHandler;
    }

    @Override
    public void handleMessage(final long[] buffer,
                              final int index,
                              final int msgSize,
                              final long timestamp,
                              final long correlationId,
                              final byte msgType) {

        // TODO mic from multiple buffers
        final long resultsCode = resultsBuffer.get(index);

        this.lastAddr = index;
        this.lastBuf = buffer;

        responseHandler.commandResult(
                timestamp,
                correlationId,
                (int) resultsCode,
                transferAccessor);


    }

    private final IPaymentsResponseHandler.ITransferAccessor transferAccessor = new IPaymentsResponseHandler.ITransferAccessor() {
        @Override
        public long getAccountFrom() {
            return lastBuf[lastAddr];
        }

        @Override
        public long getAccountTo() {
            return lastBuf[lastAddr + 1];
        }

        @Override
        public long getAmount() {
            return lastBuf[lastAddr + 2];
        }

        @Override
        public int getCurrency() {
            throw new UnsupportedOperationException();
        }
    };

}
