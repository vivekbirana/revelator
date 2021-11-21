package exchange.core2.revelator.primes;

import exchange.core2.revelator.utils.LatencyTools;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.agrona.PrintBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public final class EncryptionBenchmark {

    private static final Logger log = LoggerFactory.getLogger(EncryptionBenchmark.class);
    private static final String HMAC_SHA_256 = "HmacSHA256";
    private static final SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

    private final PrimesChecker primesChecker;

    public static void main(String[] args) throws Exception {


        final EncryptionBenchmark encryptionBenchmark = new EncryptionBenchmark();
        encryptionBenchmark.process();
    }

    public EncryptionBenchmark() {
        this.primesChecker = PrimesChecker.create();
    }


    private void process() throws NoSuchAlgorithmException, InvalidKeyException, ShortBufferException {


        final Random rand = new Random(-1L);

        byte[] secretKey = new byte[32];
        rand.nextBytes(secretKey);

        log.info("Secret: {}", PrintBufferUtil.hexDump(secretKey));

        final Mac mac = Mac.getInstance(HMAC_SHA_256);
        final SecretKeySpec secretKeySpec = new SecretKeySpec(secretKey, HMAC_SHA_256);
        mac.init(secretKeySpec);

        final byte[] messageArray = new byte[64];


        final ByteBuffer messageByteBuf = ByteBuffer.wrap(messageArray);

        for (int i = 0; i < 3; i++) {

            log.info("");

            messageByteBuf.putLong(i);
            messageByteBuf.putLong(i + 1);
            messageByteBuf.putLong(i + 2);
            messageByteBuf.putLong(i + 3);

            log.info("messageArray1 {}", PrintBufferUtil.hexDump(messageArray));

            PrintBufferUtil.hexDump(messageArray);

            mac.reset();
            mac.update(messageArray, 0, 32);
            mac.doFinal(messageArray, 32);

            log.info("messageArray2 {}", PrintBufferUtil.hexDump(messageArray));

            messageByteBuf.flip();
        }


        final int totalIterations = 5_000_000;
        final int warmupIteration = 40_000;


//        final int baseNumber = Integer.MAX_VALUE >> 1;
        final int baseNumber = Integer.MAX_VALUE - (totalIterations + 1);
//        final long baseNumber = (long) Integer.MAX_VALUE << 5;

        final List<Long> resultsNs = new ArrayList<>();

        for (int j = 0; j < 5000; j++) {

            hdrRecorder.reset();
            long totalDurationNs = 0L;

//            System.gc();
//            Thread.sleep(200);
//
//            System.gc();
//            Thread.sleep(200);

            int primes = 0;

            for (int i = 0; i < totalIterations; i++) {

                messageByteBuf.putLong(i);
                messageByteBuf.putLong(i + 1);
                messageByteBuf.putLong(i + 2);
                messageByteBuf.putLong(i + 3);

//            log.info("messageArray1 {}", PrintBufferUtil.hexDump(messageArray));

                long startTime = System.nanoTime();

                // produces garbage
                mac.reset();
                mac.update(messageArray, 0, 32);
                mac.doFinal(messageArray, 32);
//
//                Sha256.hash(messageArray);

//                if (primesChecker.isPrime(baseNumber + i)) {
//                    primes++;
//                }


                final long durationNs = (System.nanoTime() - startTime);

//            log.info("messageArray2 {}", PrintBufferUtil.hexDump(messageArray));


                if (i > warmupIteration) {
                    hdrRecorder.recordValue(durationNs);
                    totalDurationNs += durationNs;
                }

                messageByteBuf.flip();

            }

            final long resultNs = totalDurationNs / (totalIterations - warmupIteration);
            resultsNs.add(resultNs);
            final long avgNs = (long) resultsNs.stream().mapToLong(x -> x).summaryStatistics().getAverage();

            final Histogram histogram = hdrRecorder.getIntervalHistogram();
            final Map<String, String> latencyReportFast = LatencyTools.createLatencyReportFast(histogram);
//                final Map<String, String> latencyReportFast = Map.of();

            log.debug("average: {} ns (avg = {} ns), {}, primes={}", resultNs, avgNs, latencyReportFast, primes);
        }

    }


}
