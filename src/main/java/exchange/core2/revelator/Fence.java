package exchange.core2.revelator;

import jdk.internal.vm.annotation.Contended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Fence {

    private final static Logger logger = LoggerFactory.getLogger(Fence.class);

    @Contended
    protected volatile long value = -1;


    private static final long VALUE_OFFSET;


    // todo from disruptor
    static {
        try {
            VALUE_OFFSET = Revelator.UNSAFE.objectFieldOffset(Fence.class.getDeclaredField("value"));
            logger.debug("VALUE_OFFSET={}", VALUE_OFFSET);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Perform a volatile read of this sequence's value.
     *
     * @return The current value of the sequence.
     */
    public long getVolatile()
    {
        return value;
    }


    /**
     * Perform an ordered write of this sequence.  The intent is
     * a Store/Store barrier between this write and any previous
     * store.
     *
     * @param value The new value for the sequence.
     */
    public void lazySet(final long value)
    {
        Revelator.UNSAFE.putOrderedLong(this, VALUE_OFFSET, value);
    }

    /**
     * Performs a volatile write of this sequence.  The intent is
     * a Store/Store barrier between this write and any previous
     * write and a Store/Load barrier between this write and any
     * subsequent volatile read.
     *
     * @param value The new value for the sequence.
     */
    public void setVolatile(final long value)
    {
        Revelator.UNSAFE.putLongVolatile(this, VALUE_OFFSET, value);
    }


}
