package exchange.core2.revelator.processors.scalable;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.RevelatorConfig;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.LockSupport;

public final class ScalableSecondaryFlowProcessor implements IFlowProcessor {

    private static final Logger log = LoggerFactory.getLogger(ScalableSecondaryFlowProcessor.class);

    private final SimpleMessageHandler handler;

    private final IFence inboundFence;
    private final SingleWriterFence releasingFence = new SingleWriterFence();

    private final int indexMask;
    private final long[] buffer;
    private final int bufferSize;

    private long activationOffsetFrom = -1L;
    private long activationOffsetAvailable = -1L;

    private long deactivationRequestedOffset = -1L;
    private long deactivationActualOffset = -1L;

    private volatile State requestedState = State.SLEEP;

    private State actualState = State.SLEEP;

    public ScalableSecondaryFlowProcessor(final SimpleMessageHandler handler,
                                          final IFence inboundFence,
                                          final RevelatorConfig config) {

        this.handler = handler;
        this.inboundFence = inboundFence;
        this.indexMask = config.getIndexMask();
        this.buffer = config.getBuffer();
        this.bufferSize = config.getBufferSize();
    }

    @Override
    public void run() {


        long positionSeq = 0L;

        while (true) {

            final long availableSeq;
            if (actualState == State.SLEEP) {
                // note: volatile read of the current state (should be ok)
                while (requestedState != State.RUNNING) {
                    // do nothing
                    LockSupport.parkNanos(7_000);
                }
                actualState = State.RUNNING;
                positionSeq = activationOffsetFrom; // start from proposed sequence
                availableSeq = activationOffsetAvailable; // use last known from primary processor (optimization)

            } else {

                // wait for inboundFence
                long availableSeq1;
                while ((availableSeq1 = inboundFence.getAcquire(positionSeq)) <= positionSeq) {

                    Thread.yield();

                    //check requestedState sometimes
                    if (actualState == State.RUNNING) {

                        // expensive volatile read but it is acceptable if end of spike was reached
                        if (requestedState == State.FINALIZING) {
                            actualState = State.FINALIZING;
                            availableSeq1 = deactivationRequestedOffset;
                            break;
                        }
                    }

                }
                availableSeq = availableSeq1;
            }


            // currentState is running
            int queueRefreshCounter = 0;


            while (positionSeq < availableSeq) {

                final int index = (int) (positionSeq & indexMask);

                final long header1 = buffer[index];

                if (header1 == 0L) {
                    // skip until end of the buffer
                    positionSeq = (positionSeq | indexMask) + 1;
                    continue;
                }

                final long correlationId = header1 & 0x00FF_FFFF_FFFF_FFFFL;
                final int header2 = (int) (header1 >>> 56);
                final byte msgType = (byte) (header2 & 0x1F);

                if (msgType == Revelator.MSG_TYPE_POISON_PILL) {
                    log.debug("processor shutdown (received msgType={}, publishing positionSeq={}+{})", msgType, positionSeq, Revelator.MSG_HEADER_SIZE);
                    releasingFence.setRelease(positionSeq + Revelator.MSG_HEADER_SIZE);
                    handler.onShutdown();
                    return;
                }

                final long timestamp = buffer[index + 1];

                // payload size in longs
                final int payloadSize = (int) buffer[index + 2];
                final int indexMsg = index + Revelator.MSG_HEADER_SIZE;
                if (indexMsg + payloadSize > bufferSize) {
                    throw new IllegalStateException("Failed to decode message: headerSize=" + Revelator.MSG_HEADER_SIZE
                            + " payloadSize=" + payloadSize
                            + " correlationId=" + correlationId
                            + " unexpected " + (indexMsg + payloadSize - bufferSize) + " bytes");
                }

                try {
                    handler.handleMessage(buffer, indexMsg, payloadSize, timestamp, positionSeq, correlationId, msgType);
                } catch (final Exception ex) {
                    log.debug("Exception when processing batch", ex);
                    // TODO call custom handler
                }

                positionSeq += Revelator.MSG_HEADER_SIZE + payloadSize;

                // limit batches size
                if (queueRefreshCounter++ > 256) {
                    break;
                }

            }

            releasingFence.setRelease(availableSeq);
        }

    }

    public void activate(long offsetFrom, long offsetAvailable) {
        activationOffsetFrom = offsetFrom;
        activationOffsetAvailable = offsetAvailable; // just share most recent from main primary processor
        requestedState = State.RUNNING;
    }

    public void deactivate(long offset) {
        deactivationRequestedOffset = offset;
        requestedState = State.FINALIZING;
    }

    public long getActualDeactivationOffset() {
        // TODO volatile write
        return deactivationActualOffset;
    }


    public enum State {
        SLEEP,
        RUNNING,
        FINALIZING
    }


    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }

    @Override
    public String toString() {
        return "ScalableFlowProcessor{" + handler + '}';
    }
}
