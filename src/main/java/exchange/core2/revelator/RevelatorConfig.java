package exchange.core2.revelator;

public class RevelatorConfig {

    private final int indexMask;
    private final  int bufferSize;
    private final long bufferAddr;

    public RevelatorConfig(int indexMask, int bufferSize, long bufferAddr) {
        this.indexMask = indexMask;
        this.bufferSize = bufferSize;
        this.bufferAddr = bufferAddr;
    }

    public int getIndexMask() {
        return indexMask;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public long getBufferAddr() {
        return bufferAddr;
    }
}
