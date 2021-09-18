package exchange.core2.revelator.payments;

import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.processors.pipelined.PipelinedStageHandler;
import org.agrona.collections.LongHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PaymentsHandlerStage2 implements PipelinedStageHandler<TransferSession> {

    private static final Logger log = LoggerFactory.getLogger(PaymentsHandlerStage2.class);

    private final AccountsProcessor accountsProcessor;
    private final LocalResultsByteBuffer[] resultsBuffers;

    private final LongHashSet lockedAccounts;

    private final IFence[] fencesSt1;

    private final long handlersMask;

    public PaymentsHandlerStage2(AccountsProcessor accountsProcessor,
                                 LocalResultsByteBuffer[] resultsBuffers,
                                 LongHashSet lockedAccounts,
                                 IFence[] fencesSt1,
                                 long handlersMask) {

        this.accountsProcessor = accountsProcessor;
        this.resultsBuffers = resultsBuffers;
        this.lockedAccounts = lockedAccounts;
        this.fencesSt1 = fencesSt1;
        this.handlersMask = handlersMask;
    }

    @Override
    public boolean process(final TransferSession session) {

        switch (session.messageType) {

            // only transfer command can possibly require post-processing
            case PaymentsApi.CMD_TRANSFER -> {
                return processTransfer(session);
            }

            default -> {
                return true;
            }
        }

    }

    private boolean processTransfer(final TransferSession session) {

        // if message is not related to this handler - just skip it
        if (!session.processSrc && !session.processDst) {
            return true;
        }

        // source and destination both handled by this processor
        if (session.processSrc && session.processDst) {

            // no need to revert, but need unlock accounts
            lockedAccounts.remove(session.accountDst);
            lockedAccounts.remove(session.accountSrc);

            return true;
        }

        // only one account is processed by this handler
        // get status of other account  processing
        final long otherAccount = session.processSrc ? session.accountDst : session.accountSrc;
        final int otherIdx = (int) (otherAccount & handlersMask);

        // check Stage 1 progress for particular handler
        final IFence fence = fencesSt1[otherIdx];
        final long progress = fence.getAcquire(-1L);// ignore
        if (progress < session.globalOffset) { // TODO or <= ??
            // Stage 1 is not completed yet by other handler - can not progress
            return false;
        }

        // get result code for other half of transaction
        final LocalResultsByteBuffer buf = resultsBuffers[otherIdx];
        final byte otherResultCode = buf.get(session.bufferIndex);
        final long thisAccount = session.processSrc ? session.accountSrc : session.accountDst;
        if (otherResultCode != 1) {
            // other account processing failed - revert transaction
            final long amount = session.revertAmount;
            accountsProcessor.revertDeposit(
                    thisAccount,
                    session.processDst ? amount : -amount);
        }

        lockedAccounts.remove(thisAccount);
        return true;
    }

    @Override
    public int getHitWorkWeight() {
        return 5;
    }
}
