package exchange.core2.revelator.fences;

import jdk.internal.vm.annotation.Contended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class SingleWriterFence implements IFence {

    private final static Logger logger = LoggerFactory.getLogger(SingleWriterFence.class);

    @Contended
    protected volatile long value = -1;


    private static final VarHandle VALUE;

    static {
        try {
            VALUE = MethodHandles.lookup().findVarHandle(SingleWriterFence.class, "value", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Perform a acquire-read (RA-mode) of this sequence's value.
     *
     * @return The current value of the sequence.
     */
    @Override
    public long getAcquire(final long ignore) {
        return (long) VALUE.getAcquire(this);
    }

    @Override
    public long getVolatile() {
        return (long) VALUE.getVolatile(this);
    }

    @Override
    public long getOpaque() {
        return (long) VALUE.getOpaque(this);
    }

    /**
     * Perform an ordered write of this sequence.  The intent is
     * a Store/Store barrier between this write and any previous
     * store.
     *
     * @param value The new value for the sequence.
     */
    public void setRelease(final long value) {
        VALUE.setRelease(this, value);
    }

    /**
     * Performs a volatile write of this sequence.  The intent is
     * a Store/Store barrier between this write and any previous
     * write and a Store/Load barrier between this write and any
     * subsequent volatile read.
     *
     * @param value The new value for the sequence.
     */
    public void setVolatile(final long value) {
        VALUE.setVolatile(this, value);
    }


}
