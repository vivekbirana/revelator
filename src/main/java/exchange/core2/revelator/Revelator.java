package exchange.core2.revelator;

import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleFence;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import exchange.core2.revelator.processors.IFlowProcessor;
import org.agrona.BitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.LockSupport;

public final class Revelator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Revelator.class);

    public static final int MSG_HEADER_SIZE = 24;

    public static final Unsafe UNSAFE;

    static {
        try {
            final Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (NoSuchFieldException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }

    private final int bufferSize;
    private final int indexMask;
    private final long bufferAddr;

    private final List<? extends IFlowProcessor> processors;

    private final ThreadFactory threadFactory;

    private final SingleFence inboundFence; // single publisher

    private final IFence releasingFence;

    private long reservedPosition = 0L; // nextValue = single writer sequencer position in Disruptor

    private long cachedOutboundPosition = 0L; // cachedValue = min gating sequence in Disruptor

    private long tailStrike = 0L;

    public static Revelator create(final int bufferSize,
                                   final IFlowProcessorsFactory flowProcessorsFactory,
                                   final ThreadFactory threadFactory) {

        if (!BitUtil.isPowerOfTwo(bufferSize)) {
            throw new IllegalArgumentException("Revelator buffer size must be 2^N");
        }


        final SingleFence inboundFence = new SingleFence(); // single publisher

        final int indexMask = bufferSize - 1;

        final long bufferAddr = UNSAFE.allocateMemory(bufferSize);

        log.debug("bufferAddr={}", String.format("%X", bufferAddr));

        final IFlowProcessorsFactory.ProcessorsChain chain = flowProcessorsFactory.createProcessors(
                inboundFence,
                new RevelatorConfig(indexMask, bufferSize, bufferAddr));


        return new Revelator(
                bufferSize,
                indexMask,
                bufferAddr,
                chain.getProcessors(),
                threadFactory,
                inboundFence,
                chain.getReleasingFence());
    }


    private Revelator(final int bufferSize,
                      final int indexMask,
                      final long bufferAddr,
                      final List<? extends IFlowProcessor> processors,
                      final ThreadFactory threadFactory,
                      final SingleFence inboundFence,
                      final IFence outboundFence) {

        this.bufferSize = bufferSize;
        this.indexMask = indexMask;
        this.bufferAddr = bufferAddr;
        this.processors = processors;
        this.threadFactory = threadFactory;
        this.inboundFence = inboundFence;
        this.releasingFence = outboundFence;
    }

    public void start() {

        int c = 0;

        for (IFlowProcessor processor : processors) {

            final Thread thread = threadFactory.newThread(processor);
            final String threadName = "PROC-" + c;
            log.info("Starting processor {} (thread {})...", processor, threadName);
            thread.setName(threadName);
            thread.setDaemon(true);
            thread.start();
            c++;
        }
    }


    /**
     * Claim space for single message
     *
     * @param claimingPayloadSize
     * @return
     */
    public long claimSingleMessage(final int claimingPayloadSize,
                                   final long timestamp,
                                   final long correlationId,
                                   final byte messageType) {

        if (messageType < 1 || messageType > 31) {
            throw new IllegalArgumentException("message type should be in range: 1..31");
        }

        if ((correlationId >> 56) != 0) {
            throw new IllegalArgumentException("message type should be in range: 0..2^56-1");
        }

        // check message size alignment to long
        if ((claimingPayloadSize & 7) != 0) {
            throw new IllegalArgumentException("payload size should have 8*N size");
        }

        // calculate expected message size
        final int fullMessageSize = claimingPayloadSize + MSG_HEADER_SIZE;

        if (claimingPayloadSize < 0 || fullMessageSize > bufferSize) {
            throw new IllegalArgumentException("n must be >= 0 and < bufferSize");
        }

        long msgStartSequence = reservedPosition;
        this.reservedPosition += fullMessageSize;
        final long wrapPoint = this.reservedPosition - bufferSize;

        // check if new message can fit into remaining buffer
        int index = (int) (msgStartSequence & indexMask);
        final long remainingSpaceBytes = bufferSize - index;
        if (remainingSpaceBytes < fullMessageSize) {
            // can not fit - write empty message that will be ignored by headers

//            log.debug("Can not fit message beacuse msgStartSequence&mask={} fullMessageSize={} bufferSize={} : SKIP remainingSpaceBytes={}",
//                    index, fullMessageSize, bufferSize, remainingSpaceBytes);

            // write 0 message, indicating that reader should start from buffer
            // there is always at least 8 bytes available due to alignment (only need to check wrap point before writing)
            // so we check wrap point for claimed message (just to do it once)
            wrapPointCheckWaitUpdate(msgStartSequence, wrapPoint + remainingSpaceBytes);
            writeLongDataUnsafe(index, 0L);

            index = 0;
            msgStartSequence += remainingSpaceBytes;
            this.reservedPosition += remainingSpaceBytes;
        } else {

//        log.debug("msgStartSequence={} new reservedPosition={} wrapPoint={}",
//                msgStartSequence, reservedPosition, wrapPoint);

            wrapPointCheckWaitUpdate(msgStartSequence, wrapPoint);
        }

//        log.debug("WRITING HEADER correlationId={}", correlationId);

        // write header

        final long msgTypeEncoded = ((long) messageType) << 56;

        // TODO put UserCookie (4bytes), size (2bytes - 512K max msg size)

        writeLongDataUnsafe(index, msgTypeEncoded | correlationId);
        writeLongDataUnsafe(index + 8, timestamp);
        writeLongDataUnsafe(index + 16, claimingPayloadSize >> 3);

        final long payloadStartSeq = msgStartSequence + MSG_HEADER_SIZE;

//        log.debug("WRITING HEADER DONE payloadStartSeq={} index={} claimingPayloadSize={}",
//                payloadStartSeq, payloadStartSeq & indexMask, claimingPayloadSize);

        return payloadStartSeq;
    }

    private void wrapPointCheckWaitUpdate(long msgStartSequence, long wrapPoint) {

        /*                   publishedPosition            nextSequence
        ..............................|........................|.........................................................
                 |
              wrapPoint

        wrapPoint > cachedTailPosition  --- check if filling nextSequence will corrupt unreleased entries
        cachedTailPosition > publishedPosition --- "Handle the extraordinary case of a gating sequence being larger than the cursor more gracefully"
         */

        if (wrapPoint > cachedOutboundPosition) {

            // let processors progress (todo can try do once only if discovered tailFence sill not behind wrap point?)
//            log.debug("setVolatile msgStartSequence={}",msgStartSequence);
            inboundFence.setVolatile(msgStartSequence);  // StoreLoad fence

//            log.debug(" tailFence.getVolatile()={}",tailFence.getVolatile());
            long minSequence;
            while (wrapPoint > (minSequence = Math.min(releasingFence.getVolatile(cachedOutboundPosition), msgStartSequence))) {
                LockSupport.parkNanos(1L); // TODO: Use waitStrategy to spin? (can cause starvation)
//                Thread.onSpinWait();
//                Thread.yield();

                tailStrike++;
            }

            cachedOutboundPosition = minSequence;
        }
    }

    public void writeLongData(long sequence, int offset, long value) {
        final int idx = (int) sequence & indexMask;
        final long address = bufferAddr + idx + offset;
//        log.debug("WRITING LONG OFFSET={}", address - bufferAddr);
        UNSAFE.putLong(address, value);
//        log.debug("WRITING LONG DONE");
    }

    public void writeLongDataUnsafe(int index, long value) {
        UNSAFE.putLong(bufferAddr + index, value);
    }


    public void publish(long positionPlusSize) {
//        log.debug("PUBLISH positionPlusSize={}", positionPlusSize);
        inboundFence.lazySet(positionPlusSize);
        // todo waitStrategy.signalAllWhenBlocking();
    }


    public int getBufferSize() {
        return bufferSize;
    }

    public int getIndexMask() {
        return indexMask;
    }

    public long getTailStrike() {
        return tailStrike;
    }

    @Override
    public void close() throws Exception {

        UNSAFE.freeMemory(bufferAddr);

    }
}
