package exchange.core2.revelator.payments;

import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;
import jdk.internal.vm.annotation.Contended;

public final class ResponsesAggregator2 implements SimpleMessageHandler {


    private final LocalResultsByteBuffer[] resultsBuffers;
    private final IFence[] fencesSt1;
    private final IPaymentsResponseHandler responseHandler;
    private final long[] requestsBuffer;

    private final long handlersMask;

    @Contended
    private int lastAddr;

    public ResponsesAggregator2(LocalResultsByteBuffer[] resultsBuffers,
                                IFence[] fencesSt1,
                                long handlersMask,
                                IPaymentsResponseHandler responseHandler,
                                long[] requestsBuffer) {

        this.resultsBuffers = resultsBuffers;
        this.fencesSt1 = fencesSt1;
        this.responseHandler = responseHandler;
        this.handlersMask = handlersMask;
        this.requestsBuffer = requestsBuffer;
    }

    @Override
    public void handleMessage(final long[] bufferIgnore,
                              final int index,
                              final int msgSize,
                              final long timestamp,
                              final long globalOffset,
                              final long correlationId,
                              final byte msgType) {

        final long resultsCode = waitAndMergeResult(index, globalOffset, msgType);

        this.lastAddr = index;

        responseHandler.commandResult(
                timestamp,
                correlationId,
                (int) resultsCode,
                transferAccessor);
    }

    private byte waitAndMergeResult(final int index,
                                    final long globalOffset,
                                    final byte msgType) {

        final int handlerIdx1 = (int) (requestsBuffer[index] & handlersMask);

        // wait for first fence
        final IFence fence1 = fencesSt1[handlerIdx1];
        while (fence1.getAcquire(0) < globalOffset) {
            Thread.onSpinWait();
        }

        final byte result1 = resultsBuffers[handlerIdx1].get(index);
        if (msgType != PaymentsApi.CMD_TRANSFER) {
            return result1;
        }

        if (result1 < 0) {
            return result1;
        }

        final int handlerIdx2 = (int) (requestsBuffer[index + 1] & handlersMask);
        if (handlerIdx2 == handlerIdx1) {
            return result1;
        }

        // wait for second account fence
        final IFence fence2 = fencesSt1[handlerIdx2];
        while (fence2.getAcquire(0) < globalOffset) {
            Thread.onSpinWait();
        }

        return resultsBuffers[handlerIdx2].get(index);

    }

    private final IPaymentsResponseHandler.ITransferAccessor transferAccessor = new IPaymentsResponseHandler.ITransferAccessor() {
        @Override
        public long getAccountFrom() {
            return requestsBuffer[lastAddr];
        }

        @Override
        public long getAccountTo() {
            return requestsBuffer[lastAddr + 1];
        }

        @Override
        public long getAmount() {
            return requestsBuffer[lastAddr + 2];
        }

        @Override
        public int getCurrency() {
            throw new UnsupportedOperationException();
        }
    };

}
