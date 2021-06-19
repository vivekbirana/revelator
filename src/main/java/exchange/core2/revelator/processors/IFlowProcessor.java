package exchange.core2.revelator.processors;

import exchange.core2.revelator.fences.SingleFence;

public interface IFlowProcessor extends Runnable {

    SingleFence getReleasingFence();

}
