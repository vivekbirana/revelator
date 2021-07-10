package exchange.core2.revelator.processors.simple;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.RevelatorConfig;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SimpleFlowProcessor implements IFlowProcessor {

    private static final Logger log = LoggerFactory.getLogger(SimpleFlowProcessor.class);

    private final SimpleMessageHandler handler;

    private final IFence inboundFence;
    private final SingleWriterFence releasingFence;

    private final int indexMask;
    private final long bufferAddr;
    private final int bufferSize;

    private long superCounter;

    public SimpleFlowProcessor(final SimpleMessageHandler handler,
                               final IFence inboundFence,
                               final SingleWriterFence releasingFence,
                               final RevelatorConfig config) {

        this.handler = handler;
        this.inboundFence = inboundFence;
        this.releasingFence = releasingFence;
        this.indexMask = config.getIndexMask();
        this.bufferAddr = config.getBufferAddr();
        this.bufferSize = config.getBufferSize();
    }

    @Override
    public void run() {

        long positionSeq = 0L;

        while (true) {

            long availableSeq;
            int c = 0;
            while ((availableSeq = inboundFence.getVolatile(positionSeq)) <= positionSeq) {
                Thread.onSpinWait();
//                LockSupport.parkNanos(1L);
//                    if (c++ == 100) {
//                        Thread.yield();
//                        c = 0;
//                    }
//                for (int i = 0; i < 40; i++) {
//                    superCounter = (superCounter << 1) + superCounter;
//                }
//                superCounter += System.nanoTime();

            }

//        log.debug("Handle batch bufAddr={} positionSeq={} availableSeq={}", bufferAddr, positionSeq, availableSeq);

            while (positionSeq < availableSeq) {

//            log.debug("reading at index={} ({}<{})",(int) (positionSeq & indexMask), positionSeq , availableSeq);

                final long headerStartAddress = bufferAddr + (int) (positionSeq & indexMask);

                final long header1 = Revelator.UNSAFE.getLong(headerStartAddress);

                if (header1 == 0L) {
                    // skip until end of the buffer

                    final long endOfLoopSeq = (positionSeq | indexMask) + 1;
//                log.debug("Zero message: SKIP positionSeq={} -> endOfLoopSeq={} ({} bytes)", positionSeq, endOfLoopSeq, endOfLoopSeq - positionSeq);
                    positionSeq = endOfLoopSeq;
                    continue;
                }


                final long correlationId = header1 & 0x00FF_FFFF_FFFF_FFFFL;
                final int header2 = (int) (header1 >>> 56);

//            log.debug("{}", String.format("header1=%X", header1));
//            log.debug("{}", String.format("correlationId=%X = %d", correlationId, correlationId));
//            log.debug("{}", String.format("header2=%X", header2));

                final byte msgType = (byte) (header2 & 0x1F);

                if (msgType == Revelator.MSG_TYPE_POISON_PILL) {

                    log.debug("processor shutdown (received msgType={}, publishing positionSeq={})", msgType, positionSeq);
                    releasingFence.lazySet(positionSeq);
                    return;

                } else {

//            log.debug("{}", String.format("msgSizeLongsCompact=%X", msgSizeLongsCompact));
//            log.debug("{}", String.format("msgType=%X", msgType));

                    final long timestamp = Revelator.UNSAFE.getLong(headerStartAddress + 8);

                    final int payloadSize = (int) Revelator.UNSAFE.getLong(headerStartAddress + 16) << 3;
//                log.debug("custom payloadSize={}", payloadSize);

                    final long messageStartAddress = headerStartAddress + Revelator.MSG_HEADER_SIZE;
                    if (messageStartAddress + payloadSize > bufferAddr + bufferSize) {
                        throw new IllegalStateException("Failed to decode message: headerSize=" + Revelator.MSG_HEADER_SIZE
                                + " payloadSize=" + payloadSize
                                + " correlationId=" + correlationId
                                + " bufferAddr=" + bufferAddr
                                + " unexpected " + (messageStartAddress + payloadSize - bufferAddr - bufferSize) + " bytes");
                    }


                    try {
//                log.debug("Handle message messageStartAddress={} -> offsetInBuf={} payloadSize={}",
//                        messageStartAddress, headerStartAddress - bufferAddr, payloadSize);

//                Thread.sleep(1);
                        handler.handle(messageStartAddress, payloadSize, timestamp, correlationId, msgType);
//                log.debug("DONE");
                    } catch (final Exception ex) {
                        log.debug("Exception when processing batch", ex);
                        // TODO call custom handler
                    }

//            log.debug("positionSeq: {}->{} ", positionSeq, positionSeq + headerSize + payloadSize );

                    positionSeq += Revelator.MSG_HEADER_SIZE + payloadSize;
                }
            }

            releasingFence.lazySet(availableSeq);
        }

    }

    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }
}
