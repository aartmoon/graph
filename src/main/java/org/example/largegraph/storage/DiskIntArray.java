package org.example.largegraph.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public final class DiskIntArray implements Closeable {
    private static final int BYTES = Integer.BYTES;

    private final long length;
    private final int chunkSize;
    private final DiskArrayFile file;

    public DiskIntArray(Path path, long length, int chunkSize) throws IOException {
        this(path, length, chunkSize, true);
    }

    public DiskIntArray(Path path, long length, int chunkSize, boolean writable) throws IOException {
        this.length = length;
        this.chunkSize = chunkSize;
        this.file = new DiskArrayFile(path, length, BYTES, writable, true);
    }

    public void readIntChunk(
            long startId,
            int[] target,
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
            target[targetOffset + i] = buffer.getInt();
        }
    }

    public void writeIntChunk(long startId, int[] values, int requestedLength, ByteBuffer scratch) throws IOException {
        file.ensureWritable();
        file.validateRange(startId, requestedLength);
        if (values.length < requestedLength) {
            throw new IllegalArgumentException("values array is shorter than requested length");
        }
        ByteBuffer buffer = file.prepare(scratch, startId, requestedLength);
        for (int i = 0; i < requestedLength; i++) {
            buffer.putInt(values[i]);
        }
        buffer.flip();
        file.writeFully(buffer, startId);
    }

    public void fill(int value) throws IOException {
        file.truncate();
        int[] chunk = new int[(int) Math.min(chunkSize, Math.max(1L, length))];
        ByteBuffer scratch = ByteBuffer.allocate(chunk.length * BYTES);
        java.util.Arrays.fill(chunk, value);
        for (long start = 0; start < length; start += chunkSize) {
            int len = chunkLength(start);
            writeIntChunk(start, chunk, len, scratch);
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
