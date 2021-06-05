package exchange.core2.revelator;

public interface RevelatorStageHandler<S extends PipelineSession> {

    /**
     * Process single message
     *
     * @return true if processing succeeded, false to retry later
     */
    boolean process(S messageSession);

//    void handle(long msgAddr,
//                int msgSize,
//                long timestamp,
//                long correlationId,
//                byte msgType);

    int getHitWorkWeight();

}
