package exchange.core2.revelator.payments;

import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

public final class AccountsProcessor {

    private final LongLongHashMap balances = new LongLongHashMap();

    /// private static final long EXIST_FLAG = 0x8000_0000_0000_0000L;


    // private final IntLongHashMap rates = new IntLongHashMap();

//    private final int feeThresholdsCurrency = 1;
//    private final long[] feeThresholdsAmount = new long[]{0, 1000, 10_000, 100_000};
//    private final long[] fees = new long[]{3000, 2000, 1000, 500};


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

    public boolean adjustBalance(final long account, final long amount) {

        try {
            final long available = balances.get(account);

            final long newBalance = (amount >= 0)
                    ? Math.subtractExact(available, amount)
                    : Math.addExact(available, amount);

            balances.put(account, newBalance);

            return true;

        } catch (final ArithmeticException ex) {

            return false; // overflow
        }
    }

    // unsafe
    public boolean withdrawal(final long account, final long amount) {

        // decrement
        final long newBalance = balances.addToValue(account, amount);

        // should stay negative (-1 = 0)
        if (newBalance >= 0) {
            // revert
            balances.addToValue(account, -amount);
            return false;

        } else {
            return true;
        }

    }

    // unsafe
    public boolean deposit(final long account, final long amount) {

        final long newBalance = balances.addToValue(account, -amount);

        // if previous value was 0 - account did not exist
        if (newBalance == -amount) {
            // revert change
            balances.remove(account);
            return false;

        } else {
            return true;
        }
    }

    public void revertWithdrawal(final long account, final long amount) {
        balances.addToValue(account, -amount);
    }

    public void revertDeposit(final long account, final long amount) {
        balances.addToValue(account, amount);
    }

    // unsafe
    public boolean transferLocally(final long accountSrc, final long accountDst, final long amount) {

        // decrement source account balance
        final long newBalanceSrc = balances.addToValue(accountSrc, amount);

        // should stay negative (-1 value = 0 balance)
        if (newBalanceSrc >= 0) {
            // revert
            balances.addToValue(accountSrc, -amount);
            return false;
        }

        final long newBalanceDst = balances.addToValue(accountDst, -amount);

        // if previous value was 0 - account did not exist
        if (newBalanceDst == -amount) {
            // revert balance change
            balances.remove(accountDst);

            // revert source balance change
            balances.addToValue(accountSrc, -amount);
            return false;
        }

        return true;
    }

    public void openNewAccount(final long account) {
        balances.put(account, -1);
    }

    public boolean accountExists(final long account) {
        return balances.get(account) != 0;
    }

    public boolean accountHasZeroBalance(final long account) {
        return balances.get(account) == -1;
    }

    public void closeAccount(final long account) {
        balances.remove(account);
    }

    public long getBalance(final long account) {

        // not balance yet
        final long value = balances.get(account);
        if (value == 0) {
            throw new RuntimeException("Account does not exist");
        }

        return -1 - value;
    }

}
