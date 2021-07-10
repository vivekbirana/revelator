package exchange.core2.revelator.processors;

import exchange.core2.revelator.fences.IFence;
import exchange.core2.revelator.fences.SingleWriterFence;
import exchange.core2.revelator.processors.simple.SimpleMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GenericMultiProcessor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(GenericMultiProcessor.class);

    private final SimpleMessageHandler handler;

    private final IFence inboundFence;
    private final SingleWriterFence releasingFence;

    private final int indexMask;
    private final int bufferSize;
    private final long bufferAddr;


    public GenericMultiProcessor(SimpleMessageHandler handler,
                                 IFence inboundFence,
                                 SingleWriterFence releasingFence,
                                 int indexMask,
                                 int bufferSize,
                                 long bufferAddr) {

        this.handler = handler;
        this.inboundFence = inboundFence;
        this.releasingFence = releasingFence;
        this.indexMask = indexMask;
        this.bufferAddr = bufferAddr;
        this.bufferSize = bufferSize;
    }


    @Override
    public void run() {


        long lastProcessed = 0L;

        while (true) {

            // wait until new data appears
            long available = waitFenceBlocking(lastProcessed);

            process(lastProcessed, available);

            lastProcessed = available;

            releasingFence.lazySet(available);
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
        while ((available = inboundFence.getVolatile(lastProcessed)) <= lastProcessed) {
            // TODO wait strategy
            Thread.onSpinWait();
        }
        return available;
    }

    private long getFenceOnce(long lastProcessed) {
        return inboundFence.getVolatile(lastProcessed);
    }
}
