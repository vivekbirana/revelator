package exchange.core2.revelator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BatchFlowHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(BatchFlowHandler.class);

    private final Revelator revelator;
    private final StageHandler handler;

    private final Fence headFence;
    private final Fence tailFence;

    private final int indexMask;
    private final long bufferAddr;
    private final int bufferSize;

    private long superCounter;

    public BatchFlowHandler(Revelator revelator,
                            StageHandler handler,
                            Fence headFence,
                            Fence tailFence,
                            int indexMask,
                            int bufferSize,
                            long bufferAddr) {

        this.revelator = revelator;
        this.handler = handler;
        this.headFence = headFence;
        this.tailFence = tailFence;
        this.indexMask = indexMask;
        this.bufferAddr = bufferAddr;
        this.bufferSize = bufferSize;
    }

    @Override
    public void run() {

        long positionSeq = 0L;

        while (true) {

            long availableSeq;
            int c = 0;
            while ((availableSeq = headFence.getVolatile()) <= positionSeq) {
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

                final int msgSizeLongsCompact = header2 >> 5;
                final byte msgType = (byte) (header2 & 0x7);

//            log.debug("{}", String.format("msgSizeLongsCompact=%X", msgSizeLongsCompact));
//            log.debug("{}", String.format("msgType=%X", msgType));

                final long timestamp = Revelator.UNSAFE.getLong(headerStartAddress + 8);

                final int payloadSize;
                final int headerSize;
                if (msgSizeLongsCompact == 7) {
                    payloadSize = (int) Revelator.UNSAFE.getLong(headerStartAddress + 16) << 3;
//                log.debug("custom payloadSize={}", payloadSize);
                    headerSize = 24;
                } else {
                    payloadSize = msgSizeLongsCompact << 3;
//                log.debug("compact payloadSize={}", payloadSize);
                    headerSize = 16;
                }

                final long messageStartAddress = headerStartAddress + headerSize;
                if (messageStartAddress + payloadSize > bufferAddr + bufferSize) {
                    throw new IllegalStateException("Failed to decode message: headerSize=" + headerSize
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

                positionSeq += headerSize + payloadSize;
            }

            tailFence.lazySet(availableSeq);
        }

    }

}
