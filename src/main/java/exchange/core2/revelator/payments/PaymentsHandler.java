package exchange.core2.revelator.payments;

import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;

public final class PaymentsHandler implements SimpleMessageHandler {


    private final AccountsProcessor accountsProcessor;
    private final LocalResultsByteBuffer resultsBuffer;

    public PaymentsHandler(AccountsProcessor accountsProcessor,
                           LocalResultsByteBuffer resultsBuffer) {
        this.accountsProcessor = accountsProcessor;
        this.resultsBuffer = resultsBuffer;
    }

    public void handleMessage(final long[] buffer,
                              final int addr,
                              final int msgSize,
                              final long timestamp,
                              final long correlationId,
                              final byte msgType) {

//        log.debug("Handle message bufAddr={} offset={} msgSize={}", bufAddr, offset, msgSize);

        switch (msgType) {
            case PaymentsApi.CMD_TRANSFER -> {
                final long accountFrom = buffer[addr];
                final long accountTo = buffer[addr + 1];
                final long amount = buffer[addr + 2];

                final boolean success = accountsProcessor.transfer(accountFrom, accountTo, amount);
                resultsBuffer.set(addr, success ? (byte) 1 : -1);

            }

            case PaymentsApi.CMD_ADJUST -> {

                final long account = buffer[addr];
                final long amount = buffer[addr + 1];

                final boolean success = accountsProcessor.adjustBalance(account, amount);
                resultsBuffer.set(addr, success ? (byte) 1 : -1);
            }
        }


    }

}
