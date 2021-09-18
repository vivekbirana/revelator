package exchange.core2.revelator.payments;

import exchange.core2.revelator.Revelator;
import exchange.core2.revelator.buffers.LocalResultsByteBuffer;
import exchange.core2.revelator.fences.AggregatingMinFence;
import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;
import exchange.core2.revelator.processors.IFlowProcessorsFactory;
import exchange.core2.revelator.processors.ProcessorsFactories;
import exchange.core2.revelator.processors.pipelined.PipelinedFlowProcessor;
import exchange.core2.revelator.processors.simple.SimpleFlowProcessor;
import exchange.core2.revelator.utils.AffinityThreadFactory;
import org.agrona.BitUtil;
import org.agrona.collections.LongHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class PaymentsCore {

    private static final Logger log = LoggerFactory.getLogger(PaymentsCore.class);

    public final static int BUFFER_SIZE = 4 * 1024 * 1024;

    private final Revelator revelator;
    private final PaymentsApi paymentsApi;


    public static PaymentsCore createSimple(IPaymentsResponseHandler responseHandler) {

        final LocalResultsByteBuffer resultsBuffer = LocalResultsByteBuffer.create(BUFFER_SIZE);

        final AccountsProcessor accountsProcessor = new AccountsProcessor();

        final PaymentsHandler paymentsHandler = new PaymentsHandler(accountsProcessor, resultsBuffer);

        final ResponsesAggregator responsesAggregator = new ResponsesAggregator(resultsBuffer, responseHandler);

        final IFlowProcessorsFactory processorsFactory = ProcessorsFactories.chain(
                List.of(paymentsHandler, responsesAggregator));

        final AffinityThreadFactory atf = new AffinityThreadFactory(
                AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE);

        final Revelator revelator = Revelator.create(
                BUFFER_SIZE,
                processorsFactory,
                atf);

        final PaymentsApi paymentsApi = new PaymentsApi(revelator, revelator.getIndexMask());

        return new PaymentsCore(revelator, paymentsApi);
    }

    public static PaymentsCore createPipelined(IPaymentsResponseHandler responseHandler,
                                               int threadsNum) {

        if (!BitUtil.isPowerOfTwo(threadsNum)) {
            throw new IllegalArgumentException("Number of threads must be power of 2");
        }

        final long handlersMask = threadsNum - 1;

        final LocalResultsByteBuffer[] resultsBuffers = new LocalResultsByteBuffer[threadsNum];
        final IFence[] fencesSt1 = new IFence[threadsNum];

        final IFlowProcessorsFactory processorsFactory = (inboundFence, config) -> {

            final List<IFlowProcessor> processors = new ArrayList<>();

            final List<IFence> transferReleasingFences = new ArrayList<>();

            for (int i = 0; i < threadsNum; i++) {

                final LocalResultsByteBuffer resultsBuffer = LocalResultsByteBuffer.create(BUFFER_SIZE);
                resultsBuffers[i] = resultsBuffer;

                final SingleWriterFence fenceSt1 = new SingleWriterFence();
                fencesSt1[i] = fenceSt1;

                final LongHashSet lockedAccounts = new LongHashSet(20);

                final AccountsProcessor accountsProcessor = new AccountsProcessor();

                final PaymentsHandlerStage1 handlerSt1 = new PaymentsHandlerStage1(
                        accountsProcessor,
                        config.getBuffer(),
                        resultsBuffer,
                        fenceSt1,
                        lockedAccounts,
                        i,
                        handlersMask);


                final PaymentsHandlerStage2 handlerSt2 = new PaymentsHandlerStage2(
                        accountsProcessor,
                        resultsBuffers,
                        lockedAccounts,
                        fencesSt1,
                        handlersMask);


                final PipelinedFlowProcessor<TransferSession> transferProcessor = new PipelinedFlowProcessor<>(
                        List.of(handlerSt1, handlerSt2),
                        TransferSession::new,
                        inboundFence,
                        config.getIndexMask(),
                        config.getBuffer());

                processors.add(transferProcessor);

                transferReleasingFences.add(transferProcessor.getReleasingFence());

            }

            final ResponsesAggregator2 responsesAggregator = new ResponsesAggregator2(
                    resultsBuffers,
                    fencesSt1,
                    handlersMask,
                    responseHandler,
                    config.getBuffer());

            final SimpleFlowProcessor resultsProcessor = new SimpleFlowProcessor(
                    responsesAggregator,
                    new AggregatingMinFence(transferReleasingFences),
                    config);

            return new IFlowProcessorsFactory.ProcessorsChain(
                    processors,
                    resultsProcessor.getReleasingFence());
        };


        final AffinityThreadFactory atf = new AffinityThreadFactory(
                AffinityThreadFactory.ThreadAffinityMode.THREAD_AFFINITY_ENABLE_PER_PHYSICAL_CORE);

        final Revelator revelator = Revelator.create(
                BUFFER_SIZE,
                processorsFactory,
                atf);

        final PaymentsApi paymentsApi = new PaymentsApi(revelator, revelator.getIndexMask());

        return new PaymentsCore( revelator, paymentsApi);

    }


    private PaymentsCore(Revelator revelator,
                         PaymentsApi paymentsApi) {

        this.revelator = revelator;
        this.paymentsApi = paymentsApi;
    }

    public void start() {

        revelator.start();
    }

    public void stop() {

        log.info("Stopping revelator...");
        revelator.stopAsync().join();
        log.info("Revelator stopped");
    }

    public PaymentsApi getPaymentsApi() {
        return paymentsApi;
    }
}
