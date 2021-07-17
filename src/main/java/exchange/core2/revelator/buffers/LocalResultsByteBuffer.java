package exchange.core2.revelator.buffers;

import org.agrona.BitUtil;

public class LocalResultsByteBuffer {

    private final int size;
    private final byte[] buffer;
    private final long indexMask;


    public static LocalResultsByteBuffer create(int revelatorBufferSize) {

        if (!BitUtil.isPowerOfTwo(revelatorBufferSize)) {
            throw new IllegalArgumentException("buffer size must be 2^N");
        }

        final int bufferSize = revelatorBufferSize >> 4; // min message size is 24

        return new LocalResultsByteBuffer(bufferSize, new byte[bufferSize]);
    }

    private LocalResultsByteBuffer(int size, byte[] buffer) {
        this.size = size;
        this.buffer = buffer;
        this.indexMask = size - 1;
    }

    public byte get(long position) {
        return buffer[(int) ((position >> 4) & indexMask)];
    }

    public void set(long position, byte value) {
        buffer[(int) ((position >> 4) & indexMask)] = value;
    }

}
