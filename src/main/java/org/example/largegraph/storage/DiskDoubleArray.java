package org.example.largegraph.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public final class DiskDoubleArray implements Closeable {
    private static final int BYTES = Double.BYTES;
    private static final int DEFAULT_WRITE_BUFFER_BYTES = 128 * 1024;

    private final long length;
    private final int chunkSize;
    private final DiskArrayFile file;

    public DiskDoubleArray(Path path, long length, int chunkSize) throws IOException {
        this(path, length, chunkSize, true);
    }

    public DiskDoubleArray(Path path, long length, int chunkSize, boolean writable) throws IOException {
        this(path, length, chunkSize, writable, true);
    }

    public DiskDoubleArray(Path path, long length, int chunkSize, boolean writable, boolean forceOnClose) throws IOException {
        this.length = length;
        this.chunkSize = chunkSize;
        this.file = new DiskArrayFile(path, length, BYTES, writable, forceOnClose);
    }

    public void readChunk(
            long startId,
            double[] target,
            int targetOffset,
            int requestedLength,
            ByteBuffer scratch
    ) throws IOException {
        file.validateRange(startId, requestedLength);
        validateTarget(target.length, targetOffset, requestedLength);
        ByteBuffer buffer = file.prepare(scratch, startId, requestedLength);
        file.readFully(buffer, startId);
        buffer.flip();
        for (int i = 0; i < requestedLength; i++) {
            target[targetOffset + i] = buffer.getDouble();
        }
    }

    public void writeChunk(long startId, double[] values, int requestedLength) throws IOException {
        file.ensureWritable();
        file.validateRange(startId, requestedLength);
        if (values.length < requestedLength) {
            throw new IllegalArgumentException("values array is shorter than requested length");
        }
        int bufferBytes = requestedLength == 0
                ? BYTES
                : (int) Math.min(DEFAULT_WRITE_BUFFER_BYTES, Math.max((long) BYTES, (long) requestedLength * BYTES));
        ByteBuffer buffer = ByteBuffer.allocate(bufferBytes);
        long position = startId * BYTES;
        int offset = 0;
        while (offset < requestedLength) {
            buffer.clear();
            int doubles = Math.min(requestedLength - offset, buffer.capacity() / BYTES);
            for (int i = 0; i < doubles; i++) {
                buffer.putDouble(values[offset + i]);
            }
            buffer.flip();
            file.writeFully(buffer, position / BYTES);
            position += (long) doubles * BYTES;
            offset += doubles;
        }
    }

    public void fill(double value) throws IOException {
        file.truncate();
        double[] chunk = new double[(int) Math.min(chunkSize, Math.max(1L, length))];
        java.util.Arrays.fill(chunk, value);
        for (long start = 0; start < length; start += chunkSize) {
            int len = chunkLength(start);
            writeChunk(start, chunk, len);
        }
        flush();
    }

    public void flush() throws IOException {
        file.force();
    }

    private int chunkLength(long startId) {
        return (int) Math.min(chunkSize, length - startId);
    }

    private void validateTarget(int targetLength, int targetOffset, int requestedLength) {
        if (targetOffset < 0 || requestedLength < 0 || targetOffset + requestedLength > targetLength) {
            throw new IllegalArgumentException("invalid target range: offset=%d length=%d targetLength=%d"
                    .formatted(targetOffset, requestedLength, targetLength));
        }
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
