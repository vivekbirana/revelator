package exchange.core2.revelator;

import jdk.internal.vm.annotation.Contended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.LockSupport;

public final class Revelator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Revelator.class);

    private static final int maxMessageSize = 2048; // TODO parameter

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

    private final StageHandler handler;

    private final ThreadFactory threadFactory;

    private final Fence headFence = new Fence();
    private final Fence tailFence = new Fence();

    private long reservedPosition = 0L; // nextValue = single writer sequencer position in Disruptor

    private long cachedTailPosition = 0L; // cachedValue = min gating sequence in Disruptor

    private long tailStrike = 0L;

    // to avoid braking
    // number of bytes  - for current loop
    @Contended
    private int messageExtension = 0;


    public static Revelator create(final int size,
                                   final StageHandler handler,
                                   final ThreadFactory threadFactory) {


        final long bufferAddr = UNSAFE.allocateMemory(size + maxMessageSize);

//        final long l = Unsafe.getUnsafe().allocateMemory(32);


        return new Revelator(size, bufferAddr, handler, threadFactory);

    }


    private Revelator(final int bufferSize,
                      final long bufferAddr,
                      final StageHandler handler,
                      final ThreadFactory threadFactory) {

        this.bufferSize = bufferSize;
        this.bufferAddr = bufferAddr;
        this.indexMask = bufferSize - 1;
        this.handler = handler;
        this.threadFactory = threadFactory;
    }

    public void start() {

        final BatchFlowHandler batchFlowHandler = new BatchFlowHandler(
                this,
                handler,
                headFence,
                tailFence,
                indexMask,
                bufferAddr);

        Thread thread = threadFactory.newThread(batchFlowHandler);
        log.info("Starting handler...");
        thread.setName("HANDLER");
        thread.setDaemon(true);
        thread.start();
    }


    public long claim(int msgSize) {

        if (msgSize < 1 || msgSize > maxMessageSize) {
            throw new IllegalArgumentException("n must be > 0 and < maxMessageSize");
        }

        final long msgStartSequence = reservedPosition;
        reservedPosition += msgSize;
        final long wrapPoint = reservedPosition - bufferSize;

//        log.debug("msgStartSequence={} new reservedPosition={} wrapPoint={}",
//                msgStartSequence, reservedPosition, wrapPoint);

        /*                   publishedPosition            nextSequence
        ..............................|........................|.........................................................
                 |
              wrapPoint

        wrapPoint > cachedTailPosition  --- check if filling nextSequence will corrupt unreleased entries
        cachedTailPosition > publishedPosition --- "Handle the extraordinary case of a gating sequence being larger than the cursor more gracefully"
         */

        if (wrapPoint > cachedTailPosition) {

            // let processors progress (todo can try do once only if discovered tailFence sill not behind wrap point?)
//            log.debug("setVolatile msgStartSequence={}",msgStartSequence);
            headFence.setVolatile(msgStartSequence);  // StoreLoad fence

//            log.debug(" tailFence.getVolatile()={}",tailFence.getVolatile());
            long minSequence;
            while (wrapPoint > (minSequence = Math.min(tailFence.getVolatile(), msgStartSequence))) {
                LockSupport.parkNanos(1L); // TODO: Use waitStrategy to spin? (can cause starvation)
//                Thread.onSpinWait();
//                Thread.yield();

                tailStrike ++;
            }

            cachedTailPosition = minSequence;
        }

        final int extension = (int) (msgStartSequence & indexMask) + msgSize - bufferSize;

        if (extension >= 0) {
            // safe to do it because all consumers are processing between 'cachedTailPosition' and 'nextSequence'
            // messageExtension will be read only after headFence is updated by following publish operation
            // messageExtension will not be read after last handler updated tailFence
            messageExtension = extension;
        }


        return msgStartSequence;
    }

    public void writeLongData(long sequence, int offset, long value) {
        final int idx = (int) sequence & indexMask;
        UNSAFE.putLong(bufferAddr + idx + offset, value);
    }

    public void writeLongData(long offset, long value) {
        UNSAFE.putLong(bufferAddr + offset, value);
    }


    public void publish(long positionPlusSize) {
        headFence.lazySet(positionPlusSize);
        // todo waitStrategy.signalAllWhenBlocking();
    }

    int getMessageExtension(){
        return messageExtension;
    }

    public int getBufferSize(){
        return bufferSize;
    }

    public int getIndexMask(){
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
