package org.example.largegraph.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class DiskDoubleArray implements Closeable {
    private static final int BYTES = Double.BYTES;
    private static final int DEFAULT_WRITE_BUFFER_BYTES = 128 * 1024;

    private final Path path;
    private final long length;
    private final int chunkSize;
    private final FileChannel channel;
    private final boolean writable;
    private final boolean forceOnClose;

    public DiskDoubleArray(Path path, long length, int chunkSize) throws IOException {
        this(path, length, chunkSize, true);
    }

    public DiskDoubleArray(Path path, long length, int chunkSize, boolean writable) throws IOException {
        this(path, length, chunkSize, writable, true);
    }

    public DiskDoubleArray(Path path, long length, int chunkSize, boolean writable, boolean forceOnClose) throws IOException {
        this.path = path;
        this.length = length;
        this.chunkSize = chunkSize;
        this.writable = writable;
        this.forceOnClose = forceOnClose;
        if (writable && path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        this.channel = writable
                ? FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                : FileChannel.open(path, StandardOpenOption.READ);
    }

    public double[] readChunk(long startId, int requestedLength) throws IOException {
        validateRange(startId, requestedLength);
        ByteBuffer buffer = ByteBuffer.allocate(requestedLength * BYTES);
        return readChunk(startId, requestedLength, buffer);
    }

    public double[] readChunk(long startId, int requestedLength, ByteBuffer scratch) throws IOException {
        validateRange(startId, requestedLength);
        double[] values = new double[requestedLength];
        readChunk(startId, values, 0, requestedLength, scratch);
        return values;
    }

    public void readChunk(long startId, double[] target, int targetOffset, int requestedLength) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(requestedLength * BYTES);
        readChunk(startId, target, targetOffset, requestedLength, buffer);
    }

    public void readChunk(
            long startId,
            double[] target,
            int targetOffset,
            int requestedLength,
            ByteBuffer scratch
    ) throws IOException {
        validateRange(startId, requestedLength);
        validateTarget(target.length, targetOffset, requestedLength);
        ensureScratchCapacity(scratch, requestedLength);
        ByteBuffer buffer = prepareScratch(scratch, requestedLength);
        readFully(buffer, startId * BYTES);
        buffer.flip();
        for (int i = 0; i < requestedLength; i++) {
            target[targetOffset + i] = buffer.getDouble();
        }
    }

    public void writeChunk(long startId, double[] values, int requestedLength) throws IOException {
        ensureWritable();
        validateRange(startId, requestedLength);
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
            writeFully(buffer, position);
            position += (long) doubles * BYTES;
            offset += doubles;
        }
    }

    public void writeChunk(long startId, double[] values, int requestedLength, ByteBuffer scratch) throws IOException {
        ensureWritable();
        validateRange(startId, requestedLength);
        if (values.length < requestedLength) {
            throw new IllegalArgumentException("values array is shorter than requested length");
        }
        ensureScratchCapacity(scratch, requestedLength);
        ByteBuffer buffer = prepareScratch(scratch, requestedLength);
        for (int i = 0; i < requestedLength; i++) {
            buffer.putDouble(values[i]);
        }
        buffer.flip();
        writeFully(buffer, startId * BYTES);
    }

    public void fill(double value) throws IOException {
        ensureWritable();
        channel.truncate(length * BYTES);
        double[] chunk = new double[(int) Math.min(chunkSize, Math.max(1L, length))];
        ByteBuffer scratch = ByteBuffer.allocate(chunk.length * BYTES);
        java.util.Arrays.fill(chunk, value);
        for (long start = 0; start < length; start += chunkSize) {
            int len = chunkLength(start);
            writeChunk(start, chunk, len, scratch);
        }
        flush();
    }

    public void flush() throws IOException {
        if (writable) {
            channel.force(false);
        }
    }

    private int chunkLength(long startId) {
        return (int) Math.min(chunkSize, length - startId);
    }

    private void validateRange(long startId, int requestedLength) {
        if (startId < 0 || requestedLength < 0 || startId + requestedLength > length) {
            throw new IllegalArgumentException("invalid disk array range: start=%d length=%d arrayLength=%d"
                    .formatted(startId, requestedLength, length));
        }
    }

    private void ensureWritable() {
        if (!writable) {
            throw new IllegalStateException("disk array is opened read-only: " + path);
        }
    }

    private void validateTarget(int targetLength, int targetOffset, int requestedLength) {
        if (targetOffset < 0 || requestedLength < 0 || targetOffset + requestedLength > targetLength) {
            throw new IllegalArgumentException("invalid target range: offset=%d length=%d targetLength=%d"
                    .formatted(targetOffset, requestedLength, targetLength));
        }
    }

    private void ensureScratchCapacity(ByteBuffer scratch, int requestedLength) {
        int requiredBytes = requestedLength * BYTES;
        if (scratch.capacity() < requiredBytes) {
            throw new IllegalArgumentException("scratch buffer is too small: required=%d capacity=%d"
                    .formatted(requiredBytes, scratch.capacity()));
        }
    }

    private ByteBuffer prepareScratch(ByteBuffer scratch, int requestedLength) {
        scratch.clear();
        scratch.limit(requestedLength * BYTES);
        return scratch;
    }

    private void readFully(ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("unexpected EOF while reading " + path);
            }
            if (read == 0) {
                throw new IOException("zero-byte read while reading " + path);
            }
            position += read;
        }
    }

    private void writeFully(ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);
            if (written == 0) {
                throw new IOException("zero-byte write while writing " + path);
            }
            position += written;
        }
    }

    @Override
    public void close() throws IOException {
        if (forceOnClose) {
            flush();
        }
        channel.close();
    }
}
