package exchange.core2.revelator.primes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public final class PrimesChecker {

    private static final Logger log = LoggerFactory.getLogger(PrimesChecker.class);

    public static PrimesChecker create() {

        final int[] primesArray = createPrimesArray();
        return new PrimesChecker(primesArray);

    }

    private final int[] primes;

    private PrimesChecker(int[] primes) {
        this.primes = primes;
    }


    public static int[] createPrimesArray() {

        log.info("Preparing primes list...");

        int idx = 0;
//        final int maxPrime = 1 << 23;
        final int maxPrime = 1 + (int) Math.sqrt(Integer.MAX_VALUE);

        final int[] array = new int[maxPrime / 8];

        for (int i = 29; i < maxPrime; i++) {
//            if (i > 50 ? isPrime3(i) : isPrime2(i)) {
            if (isPrime2(i)) {
                array[idx++] = i;
//                if (idx == array.length) {
//                    array = Arrays.copyOf(array, array.length * 3 / 2);
//                }
            }
        }
//        PRIMES_16 = array;
        final int[] primes = Arrays.copyOf(array, idx);
        log.info("Found primes: {} (array length={}, maxPrime={})", idx, primes.length, maxPrime);

        return primes;
    }


    private static boolean isPrime2(final int x) {

        if (x <= 3) {
            return x > 1;
        }

        if ((x % 2 == 0) || (x % 3 == 0) || (x % 5 == 0)) {
            return false;
        }

        final int xSqrt = (int) Math.sqrt(x) + 1;

        int d = 5;
        while (d <= xSqrt) {
            if (x % d == 0 || x % (d + 2) == 0) {
                return false;
            }
            d += 6;
        }

        return true;
    }

    // INT    average: 773 ns  (avg = 711 ns),  {50.0%=0.0ns, 90.0%=200ns, 95.0%=9.3µs,  99.0%=13.2µs, 99.9%=13.4µs, 99.99%=18.8µs, W=31µs}, primes=46602
    // LONG   average: 2343 ns (avg = 2314 ns), {50.0%=0.0ns, 90.0%=501ns, 95.0%=28.9µs, 99.0%=41µs,   99.9%=42µs,   99.99%=51µs,   W=84µs}, primes=46602
    // LONG7  average: 2241 ns (avg = 2184 ns), {50.0%=0.0ns, 90.0%=501ns, 95.0%=27.5µs, 99.0%=40µs,   99.9%=40µs,   99.99%=55µs,   W=84µs}, primes=46602
    // LONG23 average: 2238 ns (avg = 2238 ns), {50.0%=0.0ns, 90.0%=401ns, 95.0%=27.6µs, 99.0%=40µs,   99.9%=41µs,   99.99%=52µs,   W=75µs}, primes=46602
    public boolean isPrime(final int x) {

        if (x <= 3) {
            return x > 1;
        }

        if ((x % 2 == 0) || (x % 3 == 0) || (x % 5 == 0) || (x % 7 == 0) || (x % 11 == 0)
                || (x % 13 == 0) || (x % 17 == 0) || (x % 19 == 0) || (x % 23 == 0)) {
            return false;
        }

        final int xSqrt = (int) Math.sqrt(x) + 1;

        int i = 0;
        int d;
        do {
            d = primes[i++];
            if (x % d == 0) {
                return false;
            }
        } while (d <= xSqrt && i < primes.length);
        return true;
    }


}
