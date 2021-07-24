package exchange.core2.revelator.processors.pipelined;


import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.IFlowProcessor;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/*
 * Message:
 * long sequence
 * long timestamp
 * int length
 *
 * short command
 * long uid
 * long orderId
 *
 *
 *
 *
 */
public class PipelinedFlowProcessor<S extends PipelinedFlowSession> implements IFlowProcessor {

    private static final long WORK_TRIGGER_MAX_VALUE = Long.MAX_VALUE >> 1;

    private final int numHandlers;
    private final PipelinedStageHandler<S>[] handlers;
    private final int PIPELINE_SIZE = 8; // should be be 2^N
    private final S[] sessions;

    private final IFence inboundFence;
    private final SingleWriterFence releasingFence;


    private final int indexMask;
    private final long[] buffer;


    private final int[] workWeights;
//    private final int missWeights[];


    public PipelinedFlowProcessor(final PipelinedStageHandler<S>[] handlers,
                                  final Supplier<S> sessionsFactory,
                                  final SingleWriterFence inboundFence,
                                  final SingleWriterFence releasingFence,
                                  int indexMask,
                                  long[] buffer) {

        this.handlers = handlers;
        this.numHandlers = handlers.length;
        this.sessions = createSessions(sessionsFactory, PIPELINE_SIZE);
        this.workWeights = Arrays.stream(handlers).mapToInt(PipelinedStageHandler::getHitWorkWeight).toArray();
        this.inboundFence = inboundFence;
        this.releasingFence = releasingFence;
        this.indexMask = indexMask;
        this.buffer = buffer;
    }

    @SuppressWarnings("unchecked")
    private static <S> S[] createSessions(Supplier<S> sessionsFactory, final int size) {
        return (S[]) IntStream.range(0, size)
                .mapToObj(i -> sessionsFactory.get())
                .toArray(x -> new PipelinedFlowSession[size]);
    }


    @Override
    public void run() {

        final int pipelineMask = PIPELINE_SIZE - 1;


        final long[] handlerWorkTriggers = new long[numHandlers];
//        long headWorkTrigger = 0L;
        long workCounter = 0L;

        long headSequence = -1L; // processed (last known) sequence
        long tailSequence = -1L;

        // processed (last known) sequence by each handler
        final long[] handlerSequence = new long[numHandlers];
        for (int i = 0; i < numHandlers; i++) handlerSequence[i] = -1;

        long initializerOffset = 0L;
        long availableOffset = 0L;


        while (true) {

            // always work counter
            workCounter++;

            // gatingSequence is a barrier to protect sessions queue from wrapping:
            long gatingSequence = handlerSequence[numHandlers - 1] + PIPELINE_SIZE;

            // check for new messages or init new sessions only if there some space in the pipeline
            if (tailSequence != gatingSequence) {

                // try to wait for incomingFence if it is time for that
                // availableOffset will become larger than initializerOffset if new messages arrived
//                if (workCounter >= headWorkTrigger) {

                // check fence once if batch is not completed yet (no need to spin here)
                // otherwise spin on fence volatile read
                //do {
                //  workCounter++;
                availableOffset = inboundFence.getAcquire(availableOffset);
                //Thread.onSpinWait();
                //} while (availableOffset <= initializerOffset);

                // set to known head offset
//                }

                // parse new messages if there are some
                while (initializerOffset < availableOffset) {

//            log.debug("reading at index={} ({}<{})",(int) (positionSeq & indexMask), positionSeq , availableSeq);

                    final int intdex = (int) (initializerOffset & indexMask);

                    final long header1 = buffer[intdex];

                    if (header1 == 0L) {
                        // empty message - skip until end of the buffer
                        initializerOffset = (initializerOffset | indexMask) + 1;
                        continue;
                    }

                    tailSequence++;

                    final S session = sessions[(int) tailSequence & pipelineMask];

                    session.globalOffset = initializerOffset;
                    session.correlationId = header1 & 0x00FF_FFFF_FFFF_FFFFL;

                    final int header2 = (int) (header1 >>> 56);
                    session.messageType = (byte) (header2 & 0x7);

                    // TODO throw shutdown signal exception

                    session.timestamp = buffer[intdex + 1];
                    session.payloadSize = (int) buffer[intdex + 2] << 3;

                    if (tailSequence == gatingSequence) {
                        break;
                    }

                }
            }

            // handlers cycle - each handler processed once
            // handler skip if was unsuccessfully processed recently (don't spin, do another work instead)
            long prevHandlerSequence = tailSequence;
            for (int handlerIdx = 0; handlerIdx < numHandlers; handlerIdx++) {

                long sequence = handlerSequence[handlerIdx];

                // never can go in front of previous handler
                // first handler has 'tailSequence' as a barrier, meaning it can only process messages with session
                if (sequence <= prevHandlerSequence) {

                    // check if it is time to process
                    if (workCounter >= handlerWorkTriggers[handlerIdx]) {

                        // increment work counter to indicate some work was done
                        final long workWeight = workWeights[handlerIdx];
                        workCounter += workWeight;

                        sequence++;
                        // extract next message session and try to process it
                        final S session = sessions[(int) (sequence) & pipelineMask];
                        final boolean success = handlers[handlerIdx].process(session);

                        if (success) {
                            // successful processing - move sequence forward and update it
                            handlerSequence[handlerIdx] = sequence;

                            // every time last handler makes progress - update outgoingFence
                            // TODO check if it slows down compared to publishing once-by-batch in disruptor
                            if (handlerIdx == numHandlers - 1) {
                                releasingFence.setRelease(session.globalOffset);
                            }

                        } else {
                            // not - postpone next call by some constant
                            handlerWorkTriggers[handlerIdx] = workCounter + (workWeight << 2);
                            sequence--;
                        }

                    } else {
                        // always make some progress
                        workCounter++;
                    }
                }

                prevHandlerSequence = sequence;
            }

            // work counters wrapping protection
//            if (workCounter > WORK_TRIGGER_MAX_VALUE) {
//                workCounter -= WORK_TRIGGER_MAX_VALUE;
//                for (int handlerIdx = 0; handlerIdx < numHandlers; handlerIdx++) {
//                    if (handlerWorkTriggers[handlerIdx] > 0) {
//                        handlerWorkTriggers[handlerIdx] -= WORK_TRIGGER_MAX_VALUE;
//                    }
//                }
//            }

        }


    }

    @Override
    public SingleWriterFence getReleasingFence() {
        return releasingFence;
    }


//    long waitMessage(final long lastSeq) {
//        return 0L;
//    }
//
//    long tryWaitMessage(final long lastSeq) {
//        return 0L;
//    }


}
