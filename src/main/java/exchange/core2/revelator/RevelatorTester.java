package exchange.core2.revelator;

import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

public final class RevelatorTester {

    private static final Logger log = LoggerFactory.getLogger(Revelator.class);

    public static void main(String[] args) throws InterruptedException {

        hdrRecorder.reset();

        try (final AffinityLock lock = AffinityLock.acquireCore()) {


            final AffinityThreadFactory atf = new AffinityThreadFactory(
                    AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE);

            final Revelator r = Revelator.create(bufferSizeTest, RevelatorTester::handleMessage, atf);

            r.start();

            log.debug("Starting publisher on core: {}", lock.cpuId());

            final int indexMask = r.getIndexMask();

            for (int tps = 1_000_000; tps <= 100_000_000; tps += 1_000_000) {

                latch = new CountDownLatch(1);

                final long picosPerCmd = (1024L * 1_000_000_000L) / tps;

                startTimeNs = System.nanoTime();
                final long startTimeMs = System.currentTimeMillis();

                long plannedTimestampPs = 0L;

                long expectedXorData = 0L;

                long lastKnownTimestampPs = 0L;

                final int iterations = 1_000_000;

                int nanoTimeRequestsCounter = 0;
                final long tailStrikesInitial = r.getTailStrike();

                for (int i = 0; i < iterations; i++) {

                    plannedTimestampPs += picosPerCmd;

                    while (plannedTimestampPs > lastKnownTimestampPs) {

                        lastKnownTimestampPs = (System.nanoTime() - startTimeNs) << 10;

                        nanoTimeRequestsCounter++;

                        // spin until its time to send next command
                        //Thread.onSpinWait(); // 1us-26  max34
//                        LockSupport.parkNanos(1L); // 1us-25 max29
//                         Thread.yield();   // 1us-28  max32
                    }

//            log.debug("request {}...", i);
                    final long claim = r.claim(testMsgSize);

                    final long offset = claim & indexMask;

//            log.debug("claim={}", claim);
                    long x = 0;
                    final long msg = plannedTimestampPs;
                    r.writeLongData(offset, msg);
                    for (int k = 8; k < testMsgSize; k += 8) {
                        r.writeLongData(offset + k, i);
                        x += i;
                    }

                    expectedXorData = x + (expectedXorData ^ msg);

                    r.publish(claim + testMsgSize);
                }


                //Thread.sleep(10000);

                final long claim = r.claim(testMsgSize);
                r.writeLongData(claim, 0, END_MARKER);
                r.publish(claim + testMsgSize);

                final long processingTimeMs = System.currentTimeMillis() - startTimeMs;
                final float processingTimeUs = (System.nanoTime() - startTimeNs) / 1000f;
                final float perfMt = (float) iterations / processingTimeUs;
                final float targetMt = (float) tps / 1_000_000.0f;
                String tag = String.format("%.3f (%.2fns) real:%.3f MT/s (%d-%.2f ms)",
                        targetMt, picosPerCmd / 1000.0, perfMt, processingTimeMs, processingTimeUs / 1000.0);

                latch.await();

                long tailStrikes = r.getTailStrike() - tailStrikesInitial;

                final Histogram histogram = hdrRecorder.getIntervalHistogram();
                final Map<String, String> latencyReportFast = LatencyTools.createLatencyReportFast(histogram);
//                final Map<String, String> latencyReportFast = Map.of();
                log.info("{} {} avg={} nanotimes={} tailStrikes={}", tag, latencyReportFast, (int) avgBatch, nanoTimeRequestsCounter, tailStrikes);

                if (xorData != expectedXorData) {
                    throw new IllegalStateException("Inconsistent messages");
//                    log.debug("Inconsistent messages XOR:{} processedMessages:{}", xorData, processedMessages);
//                } else {
//                    log.debug("XOR:{} processedMessages:{}", xorData, processedMessages);
                }
                xorData = 0L;
                processedMessages = 0;

//            if (histogram.getValueAtPercentile(50) > 10_000_000) {
//                break;
//            }
            }
        }

    }


    private static void handleMessage(long bufAddr, int offset, int msgSize) {

//        log.debug("Handle message bufAddr={} offset={} msgSize={}", bufAddr, offset, msgSize);

//        double a =  0.0001;
//        avgBatch = avgBatch * (1.0 - a) + msgSize * a;


        final long to = bufAddr + offset + msgSize;
        for (long addr = bufAddr + offset; addr < to; addr += testMsgSize) {

            final long msg = Revelator.UNSAFE.getLong(addr);

            if (msg == END_MARKER) {
                latch.countDown();
            } else {

                long x = 0;
                for (int k = 8; k < testMsgSize; k += 8) {
                    x += Revelator.UNSAFE.getLong(addr + k);
                }

                xorData = x + (xorData ^ msg);
//                processedMessages++;

//                if ((msg & (0x1F << 6)) == 0) {
//                if (cx++ == 3) {
                if (cx++ == 32) {
                    cx = 0;
                    final long latency = System.nanoTime() - startTimeNs - (msg >> 10);
                    hdrRecorder.recordValue(latency);
                }
            }

        }
    }

    final static int testMsgSize = 64;

    final static int bufferSizeTest = 2 * 1024 * 1024;
    final static SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

    static long xorData = 0L;
    static int processedMessages = 0;

    static long startTimeNs = 0L;
    static CountDownLatch latch;
    final static int END_MARKER = Integer.MIN_VALUE + 42;

    static long c = 0;

    static double avgBatch = 1;

    static int cx = 0;


}
