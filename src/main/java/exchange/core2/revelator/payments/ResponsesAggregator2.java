package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;
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

        final IPaymentsResponseHandler.IRequestAccessor accessor;
        switch (msgType) {
            case PaymentsApi.CMD_TRANSFER -> accessor = transferAccessor;
            case PaymentsApi.CMD_ADJUST -> accessor = adjustBalanceAccessor;
            case PaymentsApi.CMD_OPEN_ACCOUNT -> accessor = openAccountAccessor;
            case PaymentsApi.CMD_CLOSE_ACCOUNT -> accessor = closeAccountAccessor;
            case Revelator.MSG_TYPE_TEST_CONTROL -> accessor = testControlCmdAccessor;
            default -> throw new IllegalArgumentException("Unexpected message type " + msgType);
        }

        responseHandler.commandResult(
                timestamp,
                correlationId,
                (int) resultsCode,
                accessor);
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

    private final IPaymentsResponseHandler.IAdjustBalanceAccessor adjustBalanceAccessor = new IPaymentsResponseHandler.IAdjustBalanceAccessor() {
        @Override
        public byte getCommandType() {
            return PaymentsApi.CMD_ADJUST;
        }

        @Override
        public long getAccount() {
            return requestsBuffer[lastAddr];
        }

        @Override
        public long getAmount() {
            return requestsBuffer[lastAddr + 1];
        }
    };


    private final IPaymentsResponseHandler.ITransferAccessor transferAccessor = new IPaymentsResponseHandler.ITransferAccessor() {
        @Override
        public byte getCommandType() {
            return PaymentsApi.CMD_TRANSFER;
        }

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

    private final IPaymentsResponseHandler.IOpenAccountAccessor openAccountAccessor = new IPaymentsResponseHandler.IOpenAccountAccessor() {
        @Override
        public byte getCommandType() {
            return PaymentsApi.CMD_OPEN_ACCOUNT;
        }

        @Override
        public long getAccount() {
            return requestsBuffer[lastAddr];
        }
    };

    private final IPaymentsResponseHandler.ICloseAccountAccessor closeAccountAccessor = new IPaymentsResponseHandler.ICloseAccountAccessor() {
        @Override
        public byte getCommandType() {
            return PaymentsApi.CMD_CLOSE_ACCOUNT;
        }

        @Override
        public long getAccount() {
            return requestsBuffer[lastAddr];
        }
    };

    private final IPaymentsResponseHandler.ITestControlCmdAccessor testControlCmdAccessor = new IPaymentsResponseHandler.ITestControlCmdAccessor() {

        @Override
        public byte getCommandType() {
            return Revelator.MSG_TYPE_TEST_CONTROL;
        }

        @Override
        public int getMsgSize() {
            return (int) requestsBuffer[lastAddr - Revelator.MSG_HEADER_SIZE + 2];
        }

        @Override
        public byte getMsgType() {
            final long header1 = requestsBuffer[lastAddr - Revelator.MSG_HEADER_SIZE];
            final int header2 = (int) (header1 >>> 56);
            return (byte) (header2 & 0x1F);
        }

        @Override
        public long getData(int offset) {
            final int msgSize = getMsgSize();
            if (offset < 0 || offset >= msgSize) {
                throw new IllegalArgumentException("Can not request offset " + offset + " because message size is " + msgSize);
            }
            return requestsBuffer[lastAddr + offset];
        }

        @Override
        public long[] getData() {
            throw new UnsupportedOperationException(); // TODO implement
        }
    };


}
