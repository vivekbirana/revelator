package exchange.core2.revelator;

import net.openhft.affinity.AffinityLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

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

            for (int tps = 1_000_000; tps <= 70_000_000; tps += 100_000) {

                latch = new CountDownLatch(1);

                final long picosPerCmd = (1024L * 1_000_000_000L) / tps;

                startTimeNs = System.nanoTime();
                final long startTimeMs = System.currentTimeMillis();

                long plannedTimestampPs = 0L;

                long expectedXorData = 0L;

                long lastKnownTimestampPs = 0L;


                int nanoTimeRequestsCounter = 0;
                final long tailStrikesInitial = r.getTailStrike();

                for (int i = 0; i < iterationsPerTestCycle; i++) {

                    plannedTimestampPs += picosPerCmd;

                    while (plannedTimestampPs > lastKnownTimestampPs) {

                        lastKnownTimestampPs = (System.nanoTime() - startTimeNs) << 10;

                        nanoTimeRequestsCounter++;

                        // spin until its time to send next command
//                        Thread.onSpinWait(); // 1us-26  max34
//                        LockSupport.parkNanos(1L); // 1us-25 max29
//                         Thread.yield();   // 1us-28  max32
                    }

                    final long correlationId = plannedTimestampPs & 0x00FF_FFFF_FFFF_FFFFL ^ 0x0055_5555_5555_5555L;

//            log.debug("request {}...", i);
                    final long claimSeq = r.claimSingleMessage(testMsgSize, plannedTimestampPs, correlationId, (byte) 1);

                    final int index = (int) (claimSeq & indexMask);


//                    log.debug("WRITE correlationId: {}", correlationId);
//            log.debug("claimSeq={}", claimSeq);
                    long x = 0;

                    for (int k = 0; k < testMsgSize; k += 8) {
//                        log.debug("WRITE data[{}]: {}", k, i);
                        r.writeLongDataUnsafe(index + k, i);
                        x += i;
                    }

                    expectedXorData = x + (expectedXorData ^ correlationId);

//                    log.debug("expectedXorData : {}", expectedXorData);

                    r.publish(claimSeq + testMsgSize);
                }


                //Thread.sleep(10000);

                final long claim = r.claimSingleMessage(8, plannedTimestampPs, iterationsPerTestCycle, END_MARKER);
                r.writeLongData(claim, 0, 0);
                r.publish(claim + 8);

//                log.debug("FINAL PUBLISH DONE");

                final long processingTimeMs = System.currentTimeMillis() - startTimeMs;
                final float processingTimeUs = (System.nanoTime() - startTimeNs) / 1000f;
                final float perfMt = (float) iterationsPerTestCycle / processingTimeUs;
                final float targetMt = (float) tps / 1_000_000.0f;
                String tag = String.format("%.3f (%.2fns) real:%.3f MT/s (%d-%.2f ms)",
                        targetMt, picosPerCmd / 1024.0, perfMt, processingTimeMs, processingTimeUs / 1000.0);

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


    private static void handleMessage(long addr,
                                      int msgSize,
                                      long timestamp,
                                      long correlationId,
                                      byte msgType) {

//        log.debug("Handle message bufAddr={} offset={} msgSize={}", bufAddr, offset, msgSize);

//        double a =  0.0001;
//        avgBatch = avgBatch * (1.0 - a) + msgSize * a;

        if (msgType == END_MARKER) {
//            log.debug("END_MARKER MESSAGE msgSize={}", msgSize);

            latch.countDown();
        } else {

//            log.debug("READ correlationId: {}", correlationId);

            long x = 0;
            for (int k = 0; k < testMsgSize; k += 8) {
                final long data = Revelator.UNSAFE.getLong(addr + k);
                x += data;
//                log.debug("READ data[{}]: {}", k, data);
            }

            xorData = x + (xorData ^ correlationId);

//            log.debug("xorData : {}", xorData);
//                processedMessages++;

//                if ((msg & (0x1F << 6)) == 0) {
//                if (cx++ == 3) {
            if (cx++ == 32) {
                cx = 0;
                final long latency = System.nanoTime() - startTimeNs - (timestamp >> 10);
                hdrRecorder.recordValue(latency);
            }
        }
    }

    /*
    00:29:15.197 [main] INFO exchange.core2.revelator.Revelator - 1.000 (1000.00ns) real:1.000 MT/s (1000-1000.04 ms) {50.0%=0.0ns, 90.0%=100ns, 95.0%=100ns, 99.0%=2.36ms, 99.9%=5.2ms, 99.99%=5.3ms, W=5.4ms} avg=1 nanotimes=42792239 tailStrikes=0
00:29:16.200 [main] INFO exchange.core2.revelator.Revelator - 1.010 (990.10ns) real:1.010 MT/s (1003-990.11 ms) {50.0%=62ns, 90.0%=141ns, 95.0%=182ns, 99.0%=10.0µs, 99.9%=311µs, 99.99%=334µs, W=358µs} avg=1 nanotimes=43041601 tailStrikes=0
00:29:17.171 [main] INFO exchange.core2.revelator.Revelator - 1.020 (980.39ns) real:1.020 MT/s (971-980.41 ms) {50.0%=60ns, 90.0%=141ns, 95.0%=179ns, 99.0%=1.13µs, 99.9%=38µs, 99.99%=307µs, W=360µs} avg=1 nanotimes=43240621 tailStrikes=0
00:29:18.142 [main] INFO exchange.core2.revelator.Revelator - 1.030 (970.87ns) real:1.030 MT/s (971-970.88 ms) {50.0%=60ns, 90.0%=136ns, 95.0%=175ns, 99.0%=7.2µs, 99.9%=26.5µs, 99.99%=227µs, W=319µs} avg=1 nanotimes=42664732 tailStrikes=0
00:29:19.113 [main] INFO exchange.core2.revelator.Revelator - 1.040 (961.54ns) real:1.040 MT/s (971-961.54 ms) {50.0%=60ns, 90.0%=129ns, 95.0%=170ns, 99.0%=3.6µs, 99.9%=116µs, 99.99%=319µs, W=360µs} avg=1 nanotimes=42168794 tailStrikes=0
00:29:20.067 [main] INFO exchange.core2.revelator.Revelator - 1.050 (952.38ns) real:1.050 MT/s (954-952.38 ms) {50.0%=58ns, 90.0%=132ns, 95.0%=169ns, 99.0%=199ns, 99.9%=27.5µs, 99.99%=272µs, W=334µs} avg=1 nanotimes=42434049 tailStrikes=0
00:29:21.007 [main] INFO exchange.core2.revelator.Revelator - 1.060 (943.40ns) real:1.060 MT/s (940-943.40 ms) {50.0%=56ns, 90.0%=104ns, 95.0%=157ns, 99.0%=198ns, 99.9%=29.6µs, 99.99%=264µs, W=313µs} avg=1 nanotimes=42007108 tailStrikes=0
00:29:21.946 [main] INFO exchange.core2.revelator.Revelator - 1.070 (934.58ns) real:1.070 MT/s (939-934.58 ms) {50.0%=55ns, 90.0%=99ns, 95.0%=151ns, 99.0%=197ns, 99.9%=26.0µs, 99.99%=270µs, W=360µs} avg=1 nanotimes=41623982 tailStrikes=0
00:29:22.870 [main] INFO exchange.core2.revelator.Revelator - 1.080 (925.93ns) real:1.080 MT/s (924-925.93 ms) {50.0%=59ns, 90.0%=114ns, 95.0%=163ns, 99.0%=2.49µs, 99.9%=50µs, 99.99%=303µs, W=360µs} avg=1 nanotimes=41052941 tailStrikes=0
00:29:23.794 [main] INFO exchange.core2.revelator.Revelator - 1.090 (917.43ns) real:1.090 MT/s (924-917.44 ms) {50.0%=58ns, 90.0%=124ns, 95.0%=170ns, 99.0%=7.2µs, 99.9%=65µs, 99.99%=315µs, W=348µs} avg=1 nanotimes=40102581 tailStrikes=0
00:29:24.703 [main] INFO exchange.core2.revelator.Revelator - 1.100 (909.09ns) real:1.100 MT/s (909-909.09 ms) {50.0%=58ns, 90.0%=124ns, 95.0%=164ns, 99.0%=671ns, 99.9%=62µs, 99.99%=303µs, W=338µs} avg=1 nanotimes=39704684 tailStrikes=0
00:29:25.596 [main] INFO exchange.core2.revelator.Revelator - 1.110 (900.90ns) real:1.110 MT/s (893-900.91 ms) {50.0%=57ns, 90.0%=116ns, 95.0%=165ns, 99.0%=5.8µs, 99.9%=105µs, 99.99%=420µs, W=506µs} avg=1 nanotimes=39657724 tailStrikes=0


     */

    final static int testMsgSize = 48; // can be 0

    //    final static int iterationsPerTestCycle = 1488;
    final static int iterationsPerTestCycle = 1_000_000;


    final static int bufferSizeTest = 4 * 1024 * 1024;
    final static SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

    static long xorData = 0L;
    static int processedMessages = 0;

    static long startTimeNs = 0L;
    static CountDownLatch latch;
    final static byte END_MARKER = (byte) 7;

    static long c = 0;

    static double avgBatch = 1;

    static int cx = 0;


}
