package exchange.core2.revelator.payments;

import exchange.core2.benchmarks.generator.clients.ClientsCurrencyAccountsGenerator;
import exchange.core2.benchmarks.generator.currencies.CurrenciesGenerator;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public final class PaymentsTester {

    private static final Logger log = LoggerFactory.getLogger(PaymentsTester.class);


    public static void main(String[] args) {

        int seed = 1;

        final Map<Integer, Double> currencies = CurrenciesGenerator.randomCurrencies(128, 40, seed);

        log.info("Currencies: {}", currencies);

        final int accountsToCreate = 1_000_000;
        final int transfersToCreate = 3_000_000;

        final List<BitSet> clients = ClientsCurrencyAccountsGenerator.generateClients(accountsToCreate, currencies, seed);

        final List<Long> accounts = new ArrayList<>(accountsToCreate);

        for (int i = 0; i < clients.size(); i++) {

            final BitSet accountCurrencyIds = clients.get(i);
            final int clientId = i;
            final MutableInt accountNumCounter = new MutableInt(0);

            accountCurrencyIds.stream().forEach(currencyId -> {
                final int accountNum = accountNumCounter.getAndIncrement();
                long account = mapToAccount(clientId, currencyId, accountNum);
                //log.info("{}/{}/{} : {}", clientId, currencyId, accountNum, account);
                accounts.add(account);
            });
        }

        log.info("Generated {} accounts", accounts.size());

        final List<TransferTestOrder> transfers = generateTransfers(transfersToCreate, accounts, seed);


//        transferTestOrders.forEach(tx -> log.debug("{}", tx));

        log.info("Generated {} transfers", transfers.size());

        final LongLongHashMap maxBalances = createMaxBalances(transfers);

        log.info("Generated {} maxBalances", maxBalances.size());


        log.info("Creating payments core ...");
        ResponseHandler responseHandler = new ResponseHandler();

        final PaymentsCore paymentsCore = PaymentsCore.create(responseHandler);

        log.info("Starting payments core ...");
        paymentsCore.start();

        final PaymentsApi paymentsApi = paymentsCore.getPaymentsApi();

        final MutableInt correlationId = new MutableInt();

        log.info("Updating balances for {} accounts ...", maxBalances.size());

        maxBalances.forEachKeyValue((account, amount) ->
                paymentsApi.adjustBalance(System.nanoTime(), correlationId.getAndIncrement(), account, amount));

        log.info("Done");

        log.info("Performing {} transfers ...", transfers.size());

        transfers.forEach(order ->
                paymentsApi.transfer(
                        System.nanoTime(),
                        correlationId.getAndIncrement(),
                        order.sourceAccount,
                        order.destinationAccount,
                        order.amount,
                        order.currency));

        log.info("Done");

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

        @Override
        public void commandResult(long timestamp, long correlationId, int resultCode, IRequestAccessor accessor) {
            log.debug("commandResult: {}->{}", correlationId, resultCode);
        }

        @Override
        public void balanceUpdateEvent(long account, long diff, long newBalance) {
            log.debug("balanceUpdateEvent: {}->{}", account, newBalance);
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
