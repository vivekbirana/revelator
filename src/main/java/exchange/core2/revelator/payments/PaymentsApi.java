package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;

public final class PaymentsApi {


    public final static byte CMD_TRANSFER = (byte) 3;
    public final static byte CMD_ADJUST = (byte) 5;
    public final static byte QUERY_GET_BALANCE = (byte) 10;


    private final Revelator revelator;
    private final int indexMask;


    public PaymentsApi(Revelator revelator, int indexMask) {
        this.revelator = revelator;
        this.indexMask = indexMask;
    }

    public void transfer(final long timestamp,
                         final long correlationId,
                         final long accountFrom,
                         final long accountTo,
                         final long amount,
                         final int currency) {

        final int msgSize = 32;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_TRANSFER);

        final int index = (int) (claimSeq & indexMask);

        revelator.writeLongDataUnsafe(index, accountFrom);
        revelator.writeLongDataUnsafe(index + 8, accountTo);
        revelator.writeLongDataUnsafe(index + 16, amount);
        revelator.writeLongDataUnsafe(index + 24, currency);

        revelator.publish(claimSeq + msgSize);
    }

    public void adjustBalance(final long timestamp,
                              final long correlationId,
                              final long account,
                              final long amount) {

        final int msgSize = 16;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_ADJUST);

        final int index = (int) (claimSeq & indexMask);

        revelator.writeLongDataUnsafe(index, account);
        revelator.writeLongDataUnsafe(index + 8, amount);

        revelator.publish(claimSeq + msgSize);
    }


}
