package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.buffers.LocalResultsByteBuffer;

public final class PaymentsHandler {


    private final AccountsProcessor
            accountsProcessor;
    private final LocalResultsByteBuffer resultsBuffer;

    public PaymentsHandler(AccountsProcessor accountsProcessor,
                           LocalResultsByteBuffer resultsBuffer) {
        this.accountsProcessor = accountsProcessor;
        this.resultsBuffer = resultsBuffer;
    }

    public void handleMessage(final long addr,
                              final int msgSize,
                              final long timestamp,
                              final long correlationId,
                              final byte msgType) {

//        log.debug("Handle message bufAddr={} offset={} msgSize={}", bufAddr, offset, msgSize);

        switch (msgType) {

            case PaymentsApi.CMD_TRANSFER: {
                final long accountFrom = Revelator.UNSAFE.getLong(addr);
                final long accountTo = Revelator.UNSAFE.getLong(addr + 8);
                final long amount = Revelator.UNSAFE.getLong(addr + 16);

                final boolean success = accountsProcessor.transfer(accountFrom, accountTo, amount);
                resultsBuffer.set(addr, success ? (byte) 1 : -1);

                break;
            }

            case PaymentsApi.CMD_ADJUST: {

                final long account = Revelator.UNSAFE.getLong(addr);
                final long amount = Revelator.UNSAFE.getLong(addr + 8);

                final boolean success = accountsProcessor.adjustBalance(account, amount);
                resultsBuffer.set(addr, success ? (byte) 1 : -1);

                break;
            }
        }


    }

}
