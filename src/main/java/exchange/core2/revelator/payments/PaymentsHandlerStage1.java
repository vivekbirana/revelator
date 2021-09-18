package exchange.core2.revelator.payments;

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


        switch (session.messageType) {

            case PaymentsApi.CMD_TRANSFER -> {
                return processTransfer(session);
            }

            case PaymentsApi.CMD_OPEN_ACCOUNT -> {
                return processOpenAccount(session.bufferIndex);
            }

            case PaymentsApi.CMD_CLOSE_ACCOUNT -> {
                return processCloseAccount(session.bufferIndex);
            }

            case PaymentsApi.CMD_ADJUST -> {
                return processAdjustment(session.bufferIndex);
            }
        }

        return false;
    }

    private boolean processOpenAccount(final int index) {

        final long account = requestsBuffer[index];
        if (!accountsProcessor.accountExists(account)) {
            accountsProcessor.openNewAccount(account);
            resultsBuffer.set(index, (byte) 1);
        } else {
            resultsBuffer.set(index, (byte) -1);
        }

        return true;
    }

    private boolean processCloseAccount(final int index) {

        final long account = requestsBuffer[index];

        if (lockedAccounts.contains(account)) {
            // can not progress if possible rollback is expected for this account
            // that can possibly cause non-deterministic execution because of balance check
            return false;
        }

        if (!accountsProcessor.accountExists(account)) {
            // account already closed
            resultsBuffer.set(index, (byte) 2);
            return true;
        }

        if (!accountsProcessor.accountHasZeroBalance(account)) {
            // account balance is not zero
            resultsBuffer.set(index, (byte) -1);
            return true;
        }

        // can close account
        accountsProcessor.closeAccount(account);
        resultsBuffer.set(index, (byte) 1);

        return true;
    }


    private boolean processAdjustment(final int index) {

        final long account = requestsBuffer[index];
        final long amount = requestsBuffer[index + 1];

        if (lockedAccounts.contains(account)) {
            // can not progress, because non-negative check can cause non-deterministic execution
            return false;
        }

        if (!accountsProcessor.accountExists(account)) {
            // account is closed
            resultsBuffer.set(index, (byte) -1);
            return true;
        }

        boolean success = (amount > 0)
                ? accountsProcessor.deposit(account, amount)
                : accountsProcessor.withdrawal(account, -amount);

        resultsBuffer.set(index, success ? (byte) 1 : -1);
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

            st1Fence.setRelease(session.globalOffset);

        } else {

            if (!lockedAccounts.add(accountDst)) {
                // already processing this account - can not proceed with stage1
                // back-off and let stage to finalize processing
                return false;
            }

            session.revertAmount = amount;
            success = accountsProcessor.deposit(accountDst, amount);

            st1Fence.setRelease(session.globalOffset);
        }

        session.accountSrc = accountSrc;
        session.accountDst = accountDst;

        resultsBuffer.set(session.bufferIndex, success ? (byte) 1 : -1);

        return true;
    }

    @Override
    public int getHitWorkWeight() {
        return 10;
    }
}
