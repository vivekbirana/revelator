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
    private final LocalResultsByteBuffer resultsBuffer;
    private final SingleWriterFence st1Fence;

    private final long[] requestsBuffer;
    private final int handlerIndex;
    private final long handlersMask;

    private final LongHashSet lockedAccounts;

    public PaymentsHandlerStage1(AccountsProcessor accountsProcessor,
                                 long[] requestsBuffer,
                                 LocalResultsByteBuffer resultsBuffer,
                                 SingleWriterFence st1Fence,
                                 LongHashSet lockedAccounts,
                                 int handlerIndex,
                                 long handlersMask) {

        this.accountsProcessor = accountsProcessor;
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

            case PaymentsApi.CMD_ADJUST -> {
                return processAdjustment(session);
            }

            case Revelator.MSG_TYPE_TEST_CONTROL, Revelator.MSG_TYPE_POISON_PILL -> {
                resultsBuffer.set(session.bufferIndex, (byte) 42);
                st1Fence.setRelease(session.globalOffset);
                return true;
            }

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

            // account does not exists or closed
            resultsBuffer.set(session.bufferIndex, (byte) -1);
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
        final long amount = requestsBuffer[session.bufferIndex + 2];

        session.processSrc = (accountSrc & handlersMask) == handlerIndex;
        session.processDst = (accountDst & handlersMask) == handlerIndex;

        if (!session.processSrc && !session.processDst) {
            // message is not related to this handler - just skip it
            return true;
        }

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

            session.revertAmount = 0; // never need to revert
            success = accountsProcessor.transferLocally(accountSrc, accountDst, amount);

        } else if (session.processSrc) {

            if (!lockedAccounts.add(accountSrc)) {
                // already processing this account - can not proceed with stage1
                // back-off and let stage to finalize processing
                return false;

            }

            session.revertAmount = amount;
            success = accountsProcessor.withdrawal(accountSrc, amount);

        } else {

            if (!lockedAccounts.add(accountDst)) {
                // already processing this account - can not proceed with stage1
                // back-off and let stage to finalize processing
                return false;
            }

            session.revertAmount = amount;
            success = accountsProcessor.deposit(accountDst, amount);
        }

        session.accountSrc = accountSrc;
        session.accountDst = accountDst;

        if (!success) {
            log.warn("Can not process transfer {}->{}! (process {}->{})", accountSrc, accountDst, session.processSrc, session.processDst);
        }

        resultsBuffer.set(session.bufferIndex, success ? (byte) 1 : -1);

        st1Fence.setRelease(session.globalOffset);

        return true;
    }

    @Override
    public int getHitWorkWeight() {
        return 10;
    }
}
