package exchange.core2.revelator.fences;

public interface IFence {

    long getVolatile(long lastKnown);

}
