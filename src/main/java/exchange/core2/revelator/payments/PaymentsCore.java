package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import exchange.core2.revelator.processors.ProcessorsFactories;
import exchange.core2.revelator.utils.AffinityThreadFactory;

public final class PaymentsCore {


    public final static int BUFFER_SIZE = 4 * 1024 * 1024;

    private final Revelator revelator;

    private final PaymentsHandler paymentsHandler;
    private final PaymentsApi paymentsApi;


    public static PaymentsCore create() {
        final AccountsProcessor accountsProcessor = new AccountsProcessor();
        final PaymentsHandler paymentsHandler = new PaymentsHandler(accountsProcessor);

        final IFlowProcessorsFactory processorsFactory = ProcessorsFactories.single(paymentsHandler::handleMessage);

        final AffinityThreadFactory atf = new AffinityThreadFactory(
                AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE);

        final Revelator revelator = Revelator.create(
                BUFFER_SIZE,
                processorsFactory,
                atf);

        final PaymentsApi paymentsApi = new PaymentsApi(revelator, revelator.getIndexMask());

        return new PaymentsCore(paymentsHandler, revelator, paymentsApi);
    }

    private PaymentsCore(PaymentsHandler paymentsHandler,
                         Revelator revelator,
                         PaymentsApi paymentsApi) {
        this.paymentsHandler = paymentsHandler;
        this.revelator = revelator;
        this.paymentsApi = paymentsApi;
    }

    public void start() {


        revelator.start();
    }

    public void stop() throws InterruptedException {

        if (revelator != null) {
            revelator.stop();
        }
    }


}
