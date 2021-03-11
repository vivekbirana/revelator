package exchange.core2.revelator;

public interface StageHandler {

    /**
     * Allows batching
     * Can have wrapping
     *
     * @param from buffer position inclusive
     * @param to   buffer position exclusive (can be less than from)
     * @return number of bytes was actually processed? - only processor is aware valid point to break, so can not delegate to framework
     * (another option is to make milestone entries)
     */
    void handle(long bufferAddr,
                int offset,
                int msgSize);

}
