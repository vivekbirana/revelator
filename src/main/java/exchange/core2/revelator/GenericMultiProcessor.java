package exchange.core2.revelator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GenericMultiProcessor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(GenericMultiProcessor.class);

    private final Revelator revelator;
    private final StageHandler handler;

    private final Fence headFence;
    private final Fence tailFence;

    private final int indexMask;
    private final long bufferAddr;

    public GenericMultiProcessor(Revelator revelator, StageHandler handler, Fence headFence, Fence tailFence, int indexMask, long bufferAddr) {
        this.revelator = revelator;
        this.handler = handler;
        this.headFence = headFence;
        this.tailFence = tailFence;
        this.indexMask = indexMask;
        this.bufferAddr = bufferAddr;
    }

    @Override
    public void run() {


        long lastProcessed = 0L;

        while (true) {

            // wait until new data appears
            long available = waitFenceBlocking(lastProcessed);

            process(lastProcessed, available);

            lastProcessed = available;

            tailFence.lazySet(available);
        }


    }

    private void process(final long processedUpToPosition,
                         final long availablePosition) {

        final int fromIdx = (int) (processedUpToPosition & indexMask);
        final long endOfLoop = (processedUpToPosition | indexMask) + 1;
        final int toIdx = (int) (availablePosition & indexMask);

//                log.debug("Batch handler available: {} -> {} (fromIdx={} endOfLoop={} toIdx={})",
//                        position, available, fromIdx, endOfLoop, toIdx);

        try {

            if (availablePosition < endOfLoop) {// TODO < or <= ?

                // normal single piece handling
//                handler.handle(bufferAddr, fromIdx, toIdx - fromIdx);

            } else {

                // crossing buffer border


                // handle first batch
//                handler.handle(bufferAddr, fromIdx, revelator.getBufferSize() + extensionSize - fromIdx);

            }

        } catch (final Exception ex) {
            log.debug("Exception ", ex);
        }
    }

    private long waitFenceBlocking(long lastProcessed) {
        long available;
        while ((available = headFence.getVolatile()) <= lastProcessed) {
            // TODO wait strategy
            Thread.onSpinWait();
        }
        return available;
    }

    private long getFenceOnce(long lastProcessed) {
        return headFence.getVolatile();
    }
}
