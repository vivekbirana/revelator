package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import exchange.core2.revelator.processors.ProcessorsFactories;
import exchange.core2.revelator.utils.AffinityThreadFactory;

import java.util.List;

public final class PaymentsCore {


    public final static int BUFFER_SIZE = 4 * 1024 * 1024;

    private final Revelator revelator;

    private final PaymentsHandler paymentsHandler;
    private final PaymentsApi paymentsApi;


    public static PaymentsCore create(IPaymentsResponseHandler responseHandler) {

        final LocalResultsByteBuffer resultsBuffer = LocalResultsByteBuffer.create(BUFFER_SIZE);

        final AccountsProcessor accountsProcessor = new AccountsProcessor();

        final PaymentsHandler paymentsHandler = new PaymentsHandler(accountsProcessor, resultsBuffer);

        final ResponsesAggregator responsesAggregator = new ResponsesAggregator(resultsBuffer, responseHandler);

        final IFlowProcessorsFactory processorsFactory = ProcessorsFactories.chain(List.of(
                paymentsHandler::handleMessage,
                responsesAggregator::handleMessage));

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

    public PaymentsApi getPaymentsApi() {
        return paymentsApi;
    }
}
