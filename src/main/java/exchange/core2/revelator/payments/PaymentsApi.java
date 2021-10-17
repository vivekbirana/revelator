package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PaymentsApi {

    public static final byte CMD_TRANSFER = (byte) 3;
    public static final byte CMD_ADJUST = (byte) 5;
    public static final byte CMD_OPEN_ACCOUNT = (byte) 7;
    public static final byte CMD_CLOSE_ACCOUNT = (byte) 9;
    public static final byte QRY_GET_BALANCE = (byte) 10;

    private static final Logger log = LoggerFactory.getLogger(PaymentsApi.class);

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

        final int msgSize = 5;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_TRANSFER);

        final int index = (int) (claimSeq & indexMask);

        revelator.writeLongDataUnsafe(index, accountFrom);
        revelator.writeLongDataUnsafe(index + 1, accountTo);
        revelator.writeLongDataUnsafe(index + 2, amount);
        revelator.writeLongDataUnsafe(index + 3, currency);

        revelator.publish(claimSeq + msgSize);
    }

    public void openAccount(final long timestamp,
                            final long correlationId,
                            final long account) {

//        log.debug("OpenAcc >>> t={}", timestamp);

        final int msgSize = 1;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_OPEN_ACCOUNT);

//        log.debug("claimSeq={}", claimSeq);

        final int index = (int) (claimSeq & indexMask);

        revelator.writeLongDataUnsafe(index, account);

        revelator.publish(claimSeq + msgSize);

//        log.debug("published={}", claimSeq + msgSize);

    }

    public void adjustBalance(final long timestamp,
                              final long correlationId,
                              final long account,
                              final long amount) {

//        log.debug("Adjust >>> t={}", timestamp);

        final int msgSize = 2;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, CMD_ADJUST);

//        log.debug("claimSeq={}", claimSeq);

        final int index = (int) (claimSeq & indexMask);

        revelator.writeLongDataUnsafe(index, account);
        revelator.writeLongDataUnsafe(index + 1, amount);

        revelator.publish(claimSeq + msgSize);

//        log.debug("published={}", claimSeq + msgSize);

    }

    public void customQuery(final byte cmdCode,
                            final long timestamp,
                            final long correlationId,
                            final long data) {

//        log.debug("Custom >>> t={} cmdCode={}", timestamp, cmdCode);

        final int msgSize = 1;
        final long claimSeq = revelator.claimSingleMessage(msgSize, timestamp, correlationId, cmdCode);

//        log.debug("claimSeq={}", claimSeq);
        final int index = (int) (claimSeq & indexMask);
        revelator.writeLongDataUnsafe(index, data);

        revelator.publish(claimSeq + msgSize);

//        log.debug("published={}", claimSeq + msgSize);
    }

}
