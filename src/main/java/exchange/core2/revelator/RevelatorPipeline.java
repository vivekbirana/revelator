package exchange.core2.revelator;


import java.util.Arrays;

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
public class RevelatorPipeline<S> implements Runnable {


    private final int numHandlers;
    private final RevelatorStageHandler<S>[] handlers;
    private final int pipelineSize = 10;
    private final S[] sessions;

    private final Fence headFence;
    private final Fence tailFence;
//
//
//    private final int bufferSize;
//    private final int indexMask;
//    private final long bufferAddr;


    private final int workWeights[];
//    private final int missWeights[];


    public RevelatorPipeline(final RevelatorStageHandler<S>[] handlers,
                             final S[] sessions,
                             final Fence headFence,
                             final Fence tailFence) {

        this.handlers = handlers;
        this.numHandlers = handlers.length;
        this.sessions = sessions;
        this.workWeights = Arrays.stream(handlers).mapToInt(RevelatorStageHandler::getHitWorkWeight).toArray();

        this.headFence = headFence;
        this.tailFence = tailFence;
    }


    @Override
    public void run() {

        final long handlersOffset[] = new long[handlers.length];
        final long workTriggers[] = new long[handlers.length];
        long headWorkTrigger = 0L;
        long workCounter = 0L;

        long headOffset = 0;
        long tailOffset = 0;

        boolean batchCompleted = true;

        while (true) {

            // try to wait for headFence if it is time for that
            if (headWorkTrigger >= workCounter) {
                workCounter++;
                long availableOffset;
                while ((availableOffset = headFence.getVolatile()) <= headOffset) {

                    if (!batchCompleted) {
                        // incrementing work trigger for headFence
                        headWorkTrigger = workCounter + 5;
                        break;
                    }

                    Thread.onSpinWait();
                }

                // set to known head offset
                headOffset = availableOffset;
            }


            // check triggers

            int handlerToApply = numHandlers - 1;
            while (handlerToApply >= 0) {

                // previous handler offset
                final long prevHandlerOffset = handlerToApply == 0
                        ? Math.min(headOffset, handlersOffset[handlers.length - 1] + pipelineSize)
                        : handlersOffset[handlerToApply - 1];

                // can only progress if there is something to process
                if(handlersOffset[handlerToApply] < prevHandlerOffset ){
                    // should reach workCounter
                    if (workCounter > workTriggers[handlerToApply]) {
                        break;
                    }
                }

                handlerToApply--;
            }


            if (handlerToApply != -1) {
                // handlers[handlerToApply].process(  );
            }


            // read one by one
            // call handlers[0] - if return false - go to any free stage;
            //


        }


    }


//    long waitMessage(final long lastSeq) {
//        return 0L;
//    }
//
//    long tryWaitMessage(final long lastSeq) {
//        return 0L;
//    }


}
