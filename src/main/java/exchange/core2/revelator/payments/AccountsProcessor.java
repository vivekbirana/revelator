package exchange.core2.revelator.payments;

import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

public final class AccountsProcessor {

    private final LongLongHashMap balances = new LongLongHashMap();

    private final IntLongHashMap rates = new IntLongHashMap();

    private final int feeThresholdsCurrency = 1;
    private final long[] feeThresholdsAmount = new long[]{0, 1000, 10_000, 100_000};
    private final long[] fees = new long[]{3000, 2000, 1000, 500};


    public boolean transfer(final long accountFrom,
                            final long accountTo,
                            final long amount) {

        try {
            // TODO currency rate and fees

            // find first account and check NSF
            final long availableFrom = balances.get(accountFrom);
            final long fromNewBalance = Math.subtractExact(availableFrom, amount);

            if (fromNewBalance < 0) {
                return false; // NSF
            }

            // find second account
            final long balanceTo = balances.get(accountTo);
            final long toNewBalance = Math.addExact(balanceTo, amount);

            // updated both accounts balances
            balances.put(accountFrom, fromNewBalance);
            balances.put(accountTo, toNewBalance); // TODO check if exists

            return true;

        } catch (final ArithmeticException ex) {

            return false; // overflow
        }
    }


    public boolean adjustBalance(final long account,
                                 final long amount) {
        try {
            final long available = balances.get(account);
            final long newBalance = Math.subtractExact(available, amount);
            balances.put(account, newBalance);
            return true;
        } catch (final ArithmeticException ex) {

            return false; // overflow
        }

    }

}
