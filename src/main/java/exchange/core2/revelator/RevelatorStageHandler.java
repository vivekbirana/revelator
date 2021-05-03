package exchange.core2.revelator;

public interface RevelatorStageHandler<S> {

    /**
     * Process single message
     *
     * @return true if processing succeeded, false to retry later
     */
    boolean process(long timeStamp, long correlationId, int offset, int msgSize, S messageSession);

//    void handle(long msgAddr,
//                int msgSize,
//                long timestamp,
//                long correlationId,
//                byte msgType);

    int getHitWorkWeight();

}
