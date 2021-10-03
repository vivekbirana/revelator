package exchange.core2.revelator.payments;

import exchange.core2.benchmarks.generator.clients.ClientsCurrencyAccountsGenerator;
import exchange.core2.benchmarks.generator.currencies.CurrenciesGenerator;
import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.utils.LatencyTools;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class PaymentsTester {

    private static final Logger log = LoggerFactory.getLogger(PaymentsTester.class);

    private static final long SET_REFERENCE_TIME_CODE = 8121092016521817263L;
    private static final long END_BATCH_CODE = 7293651321864826354L;

    public static void main(String[] args) throws InterruptedException {
        PaymentsTester paymentsTester = new PaymentsTester();
        paymentsTester.test();

    }

    public void test() throws InterruptedException {

        int seed = 1;

        final MutableLong controlCorrelationCounter = new MutableLong();

        final Random rand = new Random(seed);

        final Map<Integer, Double> currencies = CurrenciesGenerator.randomCurrencies(128, 40, seed);

        log.info("Currencies: {}", currencies);

        final int accountsToCreate = 1_000_000;
        final int transfersToCreate = 1_000_000;

//        final int accountsToCreate = 40;
//        final int transfersToCreate = 40;


        final List<BitSet> clients = ClientsCurrencyAccountsGenerator.generateClients(accountsToCreate, currencies, seed);

        final List<Long> accounts = new ArrayList<>(accountsToCreate);

        for (int i = 0; i < clients.size(); i++) {

            final BitSet accountCurrencyIds = clients.get(i);
            final int clientId = i;
            final MutableInt accountNumCounter = new MutableInt(0);

            accountCurrencyIds.stream().forEach(currencyId -> {
                final int accountNum = accountNumCounter.getAndIncrement();
                long account = mapToAccount(clientId, currencyId, accountNum);
//                log.info("{}/{}/{} : {}", clientId, currencyId, accountNum, account);
                accounts.add(account);
            });
        }

        log.info("Generated {} accounts", accounts.size());

        final List<TransferTestOrder> transfers = generateTransfers(transfersToCreate, accounts, seed);

//        transfers.forEach(tx -> log.debug("TR: {}", tx));

        log.info("Generated {} transfers", transfers.size());

        final LongLongHashMap maxBalances = createMaxBalances(transfers);

        log.info("Generated {} maxBalances", maxBalances.size());

//        maxBalances.forEachKeyValue((acc, maxbal) -> log.debug("MAX-BAL: {}={}", acc, maxbal));

        final BlockingQueue<Long> syncQueue = new LinkedBlockingQueue<>(1);

        log.info("Creating payments core ...");
        final ResponseHandler responseHandler = new ResponseHandler(syncQueue);

//        final PaymentsCore paymentsCore = PaymentsCore.createSimple(responseHandler);
        final PaymentsCore paymentsCore = PaymentsCore.createPipelined(responseHandler, 2);

        log.info("Starting payments core ...");
        paymentsCore.start();

        final PaymentsApi paymentsApi = paymentsCore.getPaymentsApi();

        final MutableInt correlationId = new MutableInt();


        log.info("Opening all accounts...");
        accounts.forEach(account -> {
            final long timestamp = System.nanoTime();
//            log.debug("Open: {} time={}", account, timestamp);
            paymentsApi.openAccount(timestamp, correlationId.getAndIncrement(), account);
        });

        flushAndWait(controlCorrelationCounter, syncQueue, paymentsApi, System.nanoTime(), 0L);

        int iteration = 0;

        for (int tps = 1_000_000; tps <= 10_000_000; tps += 100_000 + (rand.nextInt(10000) - 5000)) {

//            log.info("Updating balances for {} accounts ...", maxBalances.size());

            MutableInt a = new MutableInt();

            maxBalances.forEachKeyValue((account, amount) -> {
//             log.debug(">>> adj {} by {}", account, amount);
                paymentsApi.adjustBalance(System.nanoTime() + a.incrementAndGet(), correlationId.getAndIncrement(), account, amount);
            });

            paymentsApi.customQuery(Revelator.MSG_TYPE_TEST_CONTROL, System.nanoTime() + a.incrementAndGet(), controlCorrelationCounter.incrementAndGet(), 0L);
            if (syncQueue.take() != controlCorrelationCounter.longValue()) {
                throw new IllegalStateException();
            }

            // System.exit(1);

//            log.info("Done");

//            log.info("Performing {} transfers ...", transfers.size());

            final long picosPerCmd = (1024L * 1_000_000_000L) / tps;

//        log.info("picosPerCmd={}", picosPerCmd);

            long plannedTimestampPs = 10_000_000L; // relative timestamp in 1/1024 ns units (~1 ps)
            long lastKnownTimestampPs = 0L;
            int nanoTimeRequestsCounter = 0;

            final long startTimeNs = System.nanoTime();

//        log.info("startTimeNs={}", startTimeNs);

            // setting timer
            flushAndWait(controlCorrelationCounter, syncQueue, paymentsApi, startTimeNs, SET_REFERENCE_TIME_CODE);


            for (final TransferTestOrder order : transfers) {

                plannedTimestampPs += picosPerCmd;

                while (plannedTimestampPs > lastKnownTimestampPs) {

                    lastKnownTimestampPs = (System.nanoTime() - startTimeNs) << 10;

                    nanoTimeRequestsCounter++;

                    if (plannedTimestampPs > lastKnownTimestampPs) {

                        // spin until its time to send next command
                        Thread.onSpinWait();
//                        Thread.yield();
//                        LockSupport.parkNanos(1);
                    }
                }

//            log.info("plannedTimestampPs={}", plannedTimestampPs);


                // TODO send batches (to benchmark processing + handler part)
                paymentsApi.transfer(
                        plannedTimestampPs,
                        correlationId.getAndIncrement(),
                        order.sourceAccount,
                        order.destinationAccount,
                        order.amount,
                        order.currency);
            }

            flushAndWait(controlCorrelationCounter, syncQueue, paymentsApi, startTimeNs, END_BATCH_CODE);

            final float processingTimeUs = (System.nanoTime() - startTimeNs) / 1000f;
            final float perfMt = (float) transfers.size() / processingTimeUs;
            final float targetMt = (float) tps / 1_000_000.0f;
            final String tag = String.format("%.2fns %.3f -> %.3f MT/s %.0f%%",
                    picosPerCmd / 1024.0, targetMt, perfMt, perfMt / targetMt * 100.0);


            final Histogram histogram = responseHandler.hdrRecorder.getIntervalHistogram();
            final Map<String, String> latencyReportFast = LatencyTools.createLatencyReportFast(histogram);
            log.info("{} {} nanotimes={}", tag, latencyReportFast, nanoTimeRequestsCounter);

//            System.exit(1);
        }

        paymentsCore.stop();

        log.info("Done");

    }

    private void flushAndWait(final MutableLong controlCorrelationCounter,
                              final BlockingQueue<Long> syncQueue,
                              final PaymentsApi paymentsApi,
                              final long correlationId,
                              final long data) throws InterruptedException {

        paymentsApi.customQuery(Revelator.MSG_TYPE_TEST_CONTROL, correlationId, controlCorrelationCounter.incrementAndGet(), data);

        if (syncQueue.take() != controlCorrelationCounter.longValue()) {
            throw new IllegalStateException();
        }
    }

    public static List<TransferTestOrder> generateTransfers(final int transfersNum,
                                                            final List<Long> accounts,
                                                            final int seed) {

        final List<TransferTestOrder> transfersList = new ArrayList<>();

        final int accountsNum = accounts.size();
        final Random random = new Random(seed);

        for (int i = 0; i < transfersNum; i++) {
            final int idxFrom = random.nextInt(accountsNum);
            final int idxToRaw = random.nextInt(accountsNum - 1);
            final int idxTo = idxToRaw < idxFrom ? idxToRaw : idxToRaw + 1;

            final long amount = random.nextInt(100_000) + 10;

            final long sourceAccount = accounts.get(idxFrom);

            final long destinationAccount = accounts.get(idxTo);
            final int currency = extractCurrency(sourceAccount);

            transfersList.add(new TransferTestOrder(sourceAccount, destinationAccount, amount, currency));
        }

        return transfersList;
    }


    public static LongLongHashMap createMaxBalances(List<TransferTestOrder> transfers) {
        final LongLongHashMap balances = new LongLongHashMap();

        transfers.forEach(tx -> {

            // TODO if currency!=source - required conversion
            // TODO fees

            balances.addToValue(tx.sourceAccount, tx.amount);

        });

        return balances;
    }

    public static IntLongHashMap createTreasures(LongLongHashMap balances) {

        final IntLongHashMap treasureBalances = new IntLongHashMap();

        balances.forEachKeyValue(
                (account, amount) ->
                        balances.addToValue(extractCurrency(account), amount));

        return treasureBalances;
    }


    private static class ResponseHandler implements IPaymentsResponseHandler {

        private final BlockingQueue<Long> syncQueue;
        private long startTimeNs = 0;

        private final SingleWriterRecorder hdrRecorder = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

        private long cx = 0;

        public ResponseHandler(BlockingQueue<Long> syncQueue) {
            this.syncQueue = syncQueue;
        }

        @Override
        public void commandResult(long timestamp,
                                  long correlationId,
                                  int resultCode,
                                  IRequestAccessor accessor) {

//            log.debug("commandResult: t={} {}->{}", timestamp, correlationId, resultCode);

            if (accessor instanceof ITestControlCmdAccessor) {

                final long operationCode = ((ITestControlCmdAccessor) accessor).getData(0);

                if (operationCode == END_BATCH_CODE) {
                    startTimeNs = 0;
                } else if (operationCode == SET_REFERENCE_TIME_CODE) {
//                log.info("received startTimeNs={}", timestamp);
                    hdrRecorder.reset();
                    startTimeNs = timestamp;
                }

                try {
                    syncQueue.put(correlationId);
                } catch (final InterruptedException ex) {
                    throw new RuntimeException(ex);
                }

            } else {

//                if (cx++ == 100) {
//                    cx = 0;

                if (startTimeNs != 0) {
                    final long nanoTime = System.nanoTime();
                    final long latency = nanoTime - startTimeNs - (timestamp >> 10);

//                    log.info("timestamp={} latency={} nanoTime={} startTimeNs={}", timestamp, latency, nanoTime, startTimeNs);

                    hdrRecorder.recordValue(latency);
                }

//                }

            }
        }

        @Override
        public void balanceUpdateEvent(long account, long diff, long newBalance) {
            // log.debug("balanceUpdateEvent: {}->{}", account, newBalance);
        }
    }

    record TransferTestOrder(
            long sourceAccount,
            long destinationAccount,
            long amount,
            int currency) {
    }
//
//    public class TransferTestOrder {
//
//        private final long sourceAccount;
//        private final long destinationAccount;
//        private final long amount;
//        private final short currency;
//
//        public TransferTestOrder(long sourceAccount, long destinationAccount, long amount, short currency) {
//            this.sourceAccount = sourceAccount;
//            this.destinationAccount = destinationAccount;
//            this.amount = amount;
//            this.currency = currency;
//        }
//
//        public long getSourceAccount() {
//            return sourceAccount;
//        }
//
//        public long getDestinationAccount() {
//            return destinationAccount;
//        }
//
//        public long getAmount() {
//            return amount;
//        }
//
//        public short getCurrency() {
//            return currency;
//        }
//
//    }


    private static long mapToAccount(long clientId, int currencyId, int accountNum) {

        if (clientId > 0x7F_FFFF_FFFFL) {
            throw new IllegalArgumentException("clientId is too big");
        }

        if (currencyId > 0xFFFF) {
            throw new IllegalArgumentException("currencyId is too big");
        }

        if (accountNum > 0xFF) {
            throw new IllegalArgumentException("accountNum is too big");
        }

        return (clientId << 24) | ((long) currencyId << 8) | accountNum;
    }

    private static int extractCurrency(long accountId) {

        return (int) (accountId >> 8) & 0xFFFF;

    }

}
