package exchange.core2.revelator;

public interface StageHandler {

    /**
     * Allows batching
     * Can have wrapping
     *
     * @param msgAddr message address
     * @param msgSize messageSize
     * @return number of bytes was actually processed? - only processor is aware valid point to break, so can not delegate to framework
     * (another option is to make milestone entries)
     */
    void handle(long msgAddr,
                int msgSize,
                long timestamp,
                long correlationId,
                byte msgType);

}
