package exchange.core2.revelator.payments;

public interface IPaymentsResponseHandler {


    void commandResult(long timestamp,
                       long correlationId,
                       int resultCode,
                       IRequestAccessor accessor);


    void balanceUpdateEvent(long account, long diff, long newBalance);


    interface IRequestAccessor {

    }

    interface ITransferAccessor extends IRequestAccessor {

        long getAccountFrom();

        long getAccountTo();

        long getAmount();

        int getCurrency();
    }

    interface IAdjustBalanceAccessor extends IRequestAccessor {

        long getAccount();

        long getAmount();
    }


}
