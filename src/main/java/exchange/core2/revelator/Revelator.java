package exchange.core2.revelator;

import jdk.internal.vm.annotation.Contended;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.locks.LockSupport;

public final class Revelator implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Revelator.class);

    private static final int maxMessageSize = 256; // TODO parameter

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

    private final Fence headFence = new Fence();
    private final Fence tailFence = new Fence();

    private long reservedPosition = 0L; // nextValue = single writer sequencer position in Disruptor

    private long cachedTailPosition = 0L; // cachedValue = min gating sequence in Disruptor

    // to avoid braking
    // number of bytes  - for current loop
    @Contended
    private int messageExtension = 0;


    public static Revelator create(final int size,
                                   final StageHandler handler) {


        final long bufferAddr = UNSAFE.allocateMemory(size + maxMessageSize);

//        final long l = Unsafe.getUnsafe().allocateMemory(32);

        return new Revelator(size, bufferAddr, handler);

    }


    private Revelator(final int bufferSize,
                      final long bufferAddr,
                      final StageHandler handler) {

        this.bufferSize = bufferSize;
        this.bufferAddr = bufferAddr;
        this.indexMask = bufferSize - 1;
        this.handler = handler;
    }

    public void start() {


        final BatchFlowHandler batchFlowHandler = new BatchFlowHandler();
        Thread thread = new Thread(batchFlowHandler);
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

    private void writeLongData(long sequence, int offset, long value) {
        final int idx = (int) sequence & indexMaskTest;
        UNSAFE.putLong(bufferAddr + idx + offset, value);
    }

    public void publish(long positionPlusSize) {
        headFence.lazySet(positionPlusSize);
        // todo waitStrategy.signalAllWhenBlocking();
    }


    private final class BatchFlowHandler implements Runnable {


        @Override
        public void run() {

            long position = 0L;

            while (true) {

                long available;
                while ((available = headFence.getVolatile()) <= position) {
                    Thread.onSpinWait();
                }


                final int fromIdx = (int) (position & indexMask);

                final long endOfLoop = (position | indexMask) + 1;

                final int toIdx = (int) (available & indexMask);


//                log.debug("Batch handler available: {} -> {} (fromIdx={} endOfLoop={} toIdx={})",
//                        position, available, fromIdx, endOfLoop, toIdx);


                try {

                    if (available < endOfLoop) {// TODO < or <= ?

                        // normal single piece handling
                        handler.handle(bufferAddr, fromIdx, toIdx - fromIdx);

                    } else {

                        // crossing buffer border

                        final int extensionSize = Revelator.this.messageExtension;

                        // handle first batch
                        handler.handle(bufferAddr, fromIdx, bufferSize + extensionSize - fromIdx);

                        if (extensionSize != toIdx) {
                            // handle second batch if exists
                            handler.handle(bufferAddr, extensionSize, toIdx - extensionSize);
                        }
                    }

                } catch (final Exception ex) {
                    log.debug("Exception ", ex);
                }

                position = available;

                tailFence.lazySet(available);
            }

        }
    }


    public static void main(String[] args) throws InterruptedException {

        hdrRecorder.reset();

        final Revelator r = Revelator.create(bufferSizeTest, Revelator::handleMessage);

        r.start();

        for (int j = 0; j < 1000; j++) {


            int tps = 10_000_000 + 100_000 * j;

            final int nanosPerCmd = 1_000_000_000 / tps;

            final long startTimeMs = System.currentTimeMillis();

            long plannedTimestamp = System.nanoTime();

            final int iterations = 1_000_000;
            for (int i = 0; i < iterations; i++) {

                plannedTimestamp += nanosPerCmd;

                while (System.nanoTime() < plannedTimestamp) {
                    // spin until its time to send next command
                    Thread.onSpinWait();
                }

//            log.debug("request {}...", i);
                final long claim = r.claim(testMsgSize);

//            log.debug("claim={}", claim);
                r.writeLongData(claim, 0, plannedTimestamp);
                for (int k = 8; k < testMsgSize; k += 8) {
                    r.writeLongData(claim, k, i);
                }

                r.publish(claim + testMsgSize);
            }


            final long processingTimeMs = System.currentTimeMillis() - startTimeMs;
            final float perfMt = (float) iterations / (float) processingTimeMs / 1000.0f;
            String tag = String.format("%.3f MT/s", perfMt);

            Thread.sleep(200);

            final Histogram histogram = hdrRecorder.getIntervalHistogram();
            log.info("{} {}", tag, LatencyTools.createLatencyReportFast(histogram));

//            if (histogram.getValueAtPercentile(50) > 10_000_000) {
//                break;
//            }
        }


    }


    final static int testMsgSize = 128;

    final static int bufferSizeTest = 256 * 1024;
    final static int indexMaskTest = bufferSizeTest - 1;
    final static SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);


    private static void handleMessage(long bufAddr, int offset, int msgSize) {

//        log.debug("Handle message bufAddr={} offset={} msgSize={}", bufAddr, offset, msgSize);


        long to = bufAddr + offset + msgSize;
        for (long addr = bufAddr + offset; addr < to; addr += testMsgSize) {


            final long msg = UNSAFE.getLong(addr);
            final long latency = System.nanoTime() - msg;
//            log.debug("msg: {}ns", msg);

            final long t = Math.min(Math.max(0L, latency), Integer.MAX_VALUE);
//            log.debug("latency: {}ns", t);
            hdrRecorder.recordValue(t);
        }
    }

    @Override
    public void close() throws Exception {

        UNSAFE.freeMemory(bufferAddr);

    }


}
