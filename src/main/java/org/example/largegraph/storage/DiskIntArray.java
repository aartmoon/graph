package org.example.largegraph.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public final class DiskIntArray implements Closeable {
    private static final int BYTES = Integer.BYTES;

    private final DiskArrayFile file;

    public DiskIntArray(Path path, long length) throws IOException {
        this(path, length, true);
    }

    public DiskIntArray(Path path, long length, boolean writable) throws IOException {
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
        DiskArrayFile.validateArrayRange(target.length, targetOffset, requestedLength);
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

    @Override
    public void close() throws IOException {
        file.close();
    }
}
