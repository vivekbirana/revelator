package exchange.core2.revelator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.LockSupport;

public final class BatchFlowHandler implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(BatchFlowHandler.class);

    private final Revelator revelator;
    private final StageHandler handler;

    private final Fence headFence;
    private final Fence tailFence;

    private final int indexMask;
    private final long bufferAddr;

    private long superCounter;

    public BatchFlowHandler(Revelator revelator,
                            StageHandler handler,
                            Fence headFence,
                            Fence tailFence,
                            int indexMask,
                            long bufferAddr) {

        this.revelator = revelator;
        this.handler = handler;
        this.headFence = headFence;
        this.tailFence = tailFence;
        this.indexMask = indexMask;
        this.bufferAddr = bufferAddr;
    }

    @Override
    public void run() {

        long position = 0L;

        while (true) {

            long available;
            int c = 0;
            while ((available = headFence.getVolatile()) <= position) {
                    Thread.onSpinWait();
//                LockSupport.parkNanos(1L);
//                    if (c++ == 100) {
//                        Thread.yield();
//                        c = 0;
//                    }
//                for (int i = 0; i < 1300; i++) {
//                    superCounter = (superCounter << 1) + superCounter;
//                }
//                superCounter += System.nanoTime();

            }


            final int fromIdx = (int) (position & indexMask);

            final long endOfLoop = (position | indexMask) + 1;

            final int toIdx = (int) (available & indexMask);


//                log.debug("Batch handler available: {} -> {} (fromIdx={} endOfLoop={} toIdx={})",
//                        position, available, fromIdx, endOfLoop, toIdx);


            try {

                if (available < endOfLoop) {// TODO < or <= ?

                    // normal single piece handling
                    handler.handle(bufferAddr, fromIdx, toIdx - fromIdx);

                } else {

                    // crossing buffer border

                    final int extensionSize = revelator.getMessageExtension();

                    // handle first batch
                    handler.handle(bufferAddr, fromIdx, revelator.getBufferSize() + extensionSize - fromIdx);

                    if (extensionSize != toIdx) {
                        // handle second batch if exists
                        handler.handle(bufferAddr, extensionSize, toIdx - extensionSize);
                    }
                }

            } catch (final Exception ex) {
                log.debug("Exception ", ex);
            }

            position = available;


            tailFence.lazySet(available);
        }

    }

}
