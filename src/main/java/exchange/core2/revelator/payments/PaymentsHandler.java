package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;

public final class PaymentsHandler {


    private final AccountsProcessor accountsProcessor;

    public PaymentsHandler(AccountsProcessor accountsProcessor) {
        this.accountsProcessor = accountsProcessor;
    }

    public void handleMessage(final long addr,
                              final int msgSize,
                              final long timestamp,
                              final long correlationId,
                              final byte msgType) {

//        log.debug("Handle message bufAddr={} offset={} msgSize={}", bufAddr, offset, msgSize);

        switch (msgType) {
            case PaymentsApi.CMD_TRANSFER:
                final long accountFrom = Revelator.UNSAFE.getLong(addr);
                final long accountTo = Revelator.UNSAFE.getLong(addr + 8);
                final long amount = Revelator.UNSAFE.getLong(addr + 16);

                accountsProcessor.transfer(accountFrom, accountTo, amount);
                break;

            case PaymentsApi.CMD_ADJUST:

                final long account = Revelator.UNSAFE.getLong(addr);
                final long adjustmentAmount = Revelator.UNSAFE.getLong(addr + 8);

                accountsProcessor.adjustBalance(account, adjustmentAmount);

        }


    }

}
