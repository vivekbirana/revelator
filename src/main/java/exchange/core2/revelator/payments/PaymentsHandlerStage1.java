package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.pipelined.PipelinedStageHandler;
import org.agrona.collections.LongHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PaymentsHandlerStage1 implements PipelinedStageHandler<TransferSession> {

    private static final Logger log = LoggerFactory.getLogger(PaymentsHandlerStage1.class);

    private final AccountsProcessor accountsProcessor;
    private final TransferFeesProcessor transferFeesProcessor;


    private final LocalResultsByteBuffer resultsBuffer;
    private final SingleWriterFence st1Fence;

    private final long[] requestsBuffer;
    private final int handlerIndex;
    private final long handlersMask;

    private final LongHashSet lockedAccounts;


//    private long useless = 0;

    public PaymentsHandlerStage1(AccountsProcessor accountsProcessor,
                                 TransferFeesProcessor transferFeesProcessor,
                                 long[] requestsBuffer,
                                 LocalResultsByteBuffer resultsBuffer,
                                 SingleWriterFence st1Fence,
                                 LongHashSet lockedAccounts,
                                 int handlerIndex,
                                 long handlersMask) {

        this.accountsProcessor = accountsProcessor;
        this.transferFeesProcessor = transferFeesProcessor;
        this.requestsBuffer = requestsBuffer;
        this.resultsBuffer = resultsBuffer;
        this.st1Fence = st1Fence;
        this.handlerIndex = handlerIndex;
        this.handlersMask = handlersMask;
        this.lockedAccounts = lockedAccounts;
    }


    @Override
    public boolean process(final TransferSession session) {

//        log.debug("ST1 t={}", session.timestamp);


        try {
            switch (session.messageType) {

                case PaymentsApi.CMD_TRANSFER -> {
                    return processTransfer(session);
                }

                case PaymentsApi.CMD_OPEN_ACCOUNT -> {
                    return processOpenAccount(session);
                }

                case PaymentsApi.CMD_CLOSE_ACCOUNT -> {
                    return processCloseAccount(session);
                }

                case PaymentsApi.CMD_ADJUST_BALANCE -> {
                    return processAdjustment(session);
                }

                case PaymentsApi.CMD_CTRL_FEES -> {
                    return processControlFeeConfig(session);
                }

                case PaymentsApi.CMD_CTRL_CUR_RATE -> {
                    return processControlCurrencyRate(session);
                }

                case Revelator.MSG_TYPE_TEST_CONTROL, Revelator.MSG_TYPE_POISON_PILL -> {
                    resultsBuffer.set(session.bufferIndex, (byte) 42);
                    st1Fence.setRelease(session.globalOffset);
                    return true;
                }

            }
        } catch (final Exception ex) {
            throw new RuntimeException("Failed to process command. " + session, ex);
        }

        throw new IllegalStateException("Unsupported message type " + session.messageType + " at offset " + session.globalOffset);
    }

    private boolean processOpenAccount(final TransferSession session) {

        final long account = requestsBuffer[session.bufferIndex];

        if ((account & handlersMask) != handlerIndex) {
            return true;
        }

        if (!accountsProcessor.accountExists(account)) {
//            log.debug("Opening account {}", account);
            accountsProcessor.openNewAccount(account);
            resultsBuffer.set(session.bufferIndex, (byte) 1);
        } else {

            log.warn("Account {} already exists!", account);
            resultsBuffer.set(session.bufferIndex, (byte) -1);
        }
        st1Fence.setRelease(session.globalOffset);

        return true;
    }

    private boolean processCloseAccount(final TransferSession session) {

        final long account = requestsBuffer[session.bufferIndex];

        if ((account & handlersMask) != handlerIndex) {
            return true;
        }

        if (lockedAccounts.contains(account)) {
            // can not progress if possible rollback is expected for this account
            // that can possibly cause non-deterministic execution because of balance check
            return false;
        }

        if (!accountsProcessor.accountExists(account)) {
            // account already closed
            resultsBuffer.set(session.bufferIndex, (byte) 2);
            st1Fence.setRelease(session.globalOffset);
            return true;
        }

        if (!accountsProcessor.accountHasZeroBalance(account)) {
            // account balance is not zero
            resultsBuffer.set(session.bufferIndex, (byte) -1);
            st1Fence.setRelease(session.globalOffset);
            return true;
        }

        // can close account
        accountsProcessor.closeAccount(account);
        resultsBuffer.set(session.bufferIndex, (byte) 1);
        st1Fence.setRelease(session.globalOffset);

        return true;
    }


    private boolean processAdjustment(final TransferSession session) {

        final long account = requestsBuffer[session.bufferIndex];

        if ((account & handlersMask) != handlerIndex) {
            return true;
        }

        final long amount = requestsBuffer[session.bufferIndex + 1];

        if (lockedAccounts.contains(account)) {
            // can not progress, because non-negative check can cause non-deterministic execution
            return false;
        }

        if (!accountsProcessor.accountExists(account)) {

            log.warn("Account {} does not exists or closed!", account);
//            log.warn("Useless {} ", useless);

            // account does not exist or closed
            resultsBuffer.set(session.bufferIndex, (byte) -2);
//            log.debug("st1Fence.setRelease({})", session.globalOffset);
            st1Fence.setRelease(session.globalOffset);
            return true;
        }

        boolean success = (amount > 0)
                ? accountsProcessor.deposit(account, amount)
                : accountsProcessor.withdrawal(account, -amount);

        resultsBuffer.set(session.bufferIndex, success ? (byte) 1 : -1);
//        log.debug("st1Fence.setRelease({})", session.globalOffset);
        st1Fence.setRelease(session.globalOffset);
        return true;
    }

    private boolean processTransfer(final TransferSession session) {

        final long accountSrc = requestsBuffer[session.bufferIndex];
        final long accountDst = requestsBuffer[session.bufferIndex + 1];

        session.processSrc = (accountSrc & handlersMask) == handlerIndex;
        session.processDst = (accountDst & handlersMask) == handlerIndex;

        if (!session.processSrc && !session.processDst) {
            // message is not related to this handler - just skip it
            return true;
        }

        final long msgAmount = requestsBuffer[session.bufferIndex + 2];
        session.msgAmount = msgAmount;

        final long ttAndCurr = requestsBuffer[session.bufferIndex + 3];
        session.transferType = TransferType.fromByte((byte) ttAndCurr);

//        int h = Hashing.hash(accountDst);
//        for (int i = 0; i < 60; i++) {
//            h = Hashing.hash(h);
//        }
//        useless += h;

        final short currencySrc = AccountsProcessor.extractCurrency(accountSrc);
        final short currencyDst = AccountsProcessor.extractCurrency(accountDst);

        final boolean success;

        if (session.processSrc && session.processDst) {
            // source and destination both handled by this processor
            // blocking both accounts

            if (!lockedAccounts.add(accountSrc)) {
                // source already locked
                return false;
            }

            if (!lockedAccounts.add(accountDst)) {
                // destination already locked - unblock source
                lockedAccounts.remove(accountSrc);
                return false;
            }

            // no St2-revert scenario possible for local transfer
            session.revertAmount = 0;

            switch (session.transferType) {

                case DESTINATION_EXACT -> {

                    final long amountSrc = transferFeesProcessor.processDstExact(msgAmount, currencySrc, currencyDst);
                    success = accountsProcessor.transferLocally(accountSrc, accountDst, amountSrc, msgAmount);
                    if (!success) {
                        transferFeesProcessor.revertDstExact(msgAmount, currencySrc, currencyDst);
                    }

                }

                case SOURCE_EXACT -> {

                    final long amountDst = transferFeesProcessor.processSrcExact(msgAmount, currencySrc, currencyDst);

                    if (amountDst > 0) {
                        success = accountsProcessor.transferLocally(accountSrc, accountDst, msgAmount, amountDst);
                        if (!success) {
                            transferFeesProcessor.revertSrcExact(msgAmount, currencySrc, currencyDst);
                        }
                    } else {
                        success = false;
                    }

                }

                default -> throw new IllegalStateException("Unsupported transfer type: " + session.transferType);
            }


        } else if (session.processSrc) {
            // process only Source account

            if (!lockedAccounts.add(accountSrc)) {
                // already processing this account - can not proceed with stage1
                // back-off and let stage to finalize processing
                return false;

            }


            switch (session.transferType) {

                case DESTINATION_EXACT -> {

                    final long amountSrc = transferFeesProcessor.processDstExact(msgAmount, currencySrc, currencyDst);
                    session.revertAmount = amountSrc;
                    success = accountsProcessor.withdrawal(accountSrc, amountSrc);
                    if (!success) {
                        transferFeesProcessor.revertDstExact(msgAmount, currencySrc, currencyDst);
                    }
                }

                case SOURCE_EXACT -> {
                    session.revertAmount = msgAmount;
                    success = accountsProcessor.withdrawal(accountSrc, msgAmount);
                }

                default -> throw new IllegalStateException("Unsupported transfer type: " + session.transferType);
            }


        } else {
            // process only Destination account

            if (!lockedAccounts.add(accountDst)) {
                // already processing this account - can not proceed with stage1
                // back-off and let stage to finalize processing
                return false;
            }

            switch (session.transferType) {

                case DESTINATION_EXACT -> {

                    session.revertAmount = msgAmount;
                    success = accountsProcessor.deposit(accountDst, msgAmount);
                }

                case SOURCE_EXACT -> {

                    final long amountDst = transferFeesProcessor.processSrcExact(msgAmount, currencySrc, currencyDst);

                    if (amountDst > 0) {
                        session.revertAmount = amountDst;
                        success = accountsProcessor.deposit(accountDst, amountDst);
                        if (!success) {
                            transferFeesProcessor.revertSrcExact(msgAmount, currencySrc, currencyDst);
                        }
                    } else {
                        success = false;
                    }
                }

                default -> throw new IllegalStateException("Unsupported transfer type: " + session.transferType);
            }
        }

        session.accountSrc = accountSrc;
        session.accountDst = accountDst;

        if (!success) {
            log.warn("Can not process transfer {}->{}! (process {}->{}) {}", accountSrc, accountDst, session.processSrc, session.processDst, session.transferType);
        }

        resultsBuffer.set(session.bufferIndex, success ? (byte) 1 : -1);

        st1Fence.setRelease(session.globalOffset);

        return true;
    }

    private boolean processControlCurrencyRate(final TransferSession session) {

        final long currencies = requestsBuffer[session.bufferIndex];
        final short currencyFrom = (short) (currencies >> 32);
        final short currencyTo = (short) (currencies & Integer.MAX_VALUE);

        final double rate = Double.longBitsToDouble(requestsBuffer[session.bufferIndex + 1]);

        transferFeesProcessor.updateCurrencyRate(currencyFrom, currencyTo, rate);

        return true;
    }


    private boolean processControlFeeConfig(final TransferSession session) {


        final double feeK = Double.longBitsToDouble(requestsBuffer[session.bufferIndex]);
        transferFeesProcessor.setFeeK(feeK);

        for (int i = 1; i < session.payloadSize; i += 3) {

            final short currency = (short) requestsBuffer[session.bufferIndex + i];
            final long minFee = requestsBuffer[session.bufferIndex + i + 1];
            final long maxFee = requestsBuffer[session.bufferIndex + i + 2];

            transferFeesProcessor.putFeeConfig(currency, minFee, maxFee);
        }

        return true;
    }


    @Override
    public int getHitWorkWeight() {
        return 10;
    }

    @Override
    public String toString() {
        return "ST1(" + handlerIndex + ')';
    }
}
