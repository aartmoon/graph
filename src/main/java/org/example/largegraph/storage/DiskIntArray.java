package org.example.largegraph.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class DiskIntArray implements Closeable {
    private static final int BYTES = Integer.BYTES;

    private final Path path;
    private final long length;
    private final int chunkSize;
    private final FileChannel channel;
    private final boolean writable;
    private final boolean forceOnClose;

    private long cachedChunkStart = -1;
    private int cachedChunkLength;
    private int[] cachedChunk;
    private boolean cacheDirty;

    public DiskIntArray(Path path, long length, int chunkSize) throws IOException {
        this(path, length, chunkSize, true);
    }

    public DiskIntArray(Path path, long length, int chunkSize, boolean writable) throws IOException {
        this(path, length, chunkSize, writable, true);
    }

    public DiskIntArray(Path path, long length, int chunkSize, boolean writable, boolean forceOnClose) throws IOException {
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

    public int[] readIntChunk(long startId, int requestedLength) throws IOException {
        validateRange(startId, requestedLength);
        ByteBuffer buffer = ByteBuffer.allocate(requestedLength * BYTES);
        return readIntChunk(startId, requestedLength, buffer);
    }

    public int[] readIntChunk(long startId, int requestedLength, ByteBuffer scratch) throws IOException {
        validateRange(startId, requestedLength);
        int[] values = new int[requestedLength];
        readIntChunk(startId, values, 0, requestedLength, scratch);
        return values;
    }

    public void readIntChunk(long startId, int[] target, int targetOffset, int requestedLength) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(requestedLength * BYTES);
        readIntChunk(startId, target, targetOffset, requestedLength, buffer);
    }

    public void readIntChunk(
            long startId,
            int[] target,
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
            target[targetOffset + i] = buffer.getInt();
        }
    }

    public void writeIntChunk(long startId, int[] values, int requestedLength) throws IOException {
        ensureWritable();
        validateRange(startId, requestedLength);
        if (values.length < requestedLength) {
            throw new IllegalArgumentException("values array is shorter than requested length");
        }
        ByteBuffer buffer = ByteBuffer.allocate(requestedLength * BYTES);
        writeIntChunk(startId, values, requestedLength, buffer);
    }

    public void writeIntChunk(long startId, int[] values, int requestedLength, ByteBuffer scratch) throws IOException {
        ensureWritable();
        validateRange(startId, requestedLength);
        if (values.length < requestedLength) {
            throw new IllegalArgumentException("values array is shorter than requested length");
        }
        ensureScratchCapacity(scratch, requestedLength);
        ByteBuffer buffer = prepareScratch(scratch, requestedLength);
        for (int i = 0; i < requestedLength; i++) {
            buffer.putInt(values[i]);
        }
        buffer.flip();
        writeFully(buffer, startId * BYTES);
    }

    public void fill(int value) throws IOException {
        ensureWritable();
        flushCachedChunk();
        channel.truncate(length * BYTES);
        int[] chunk = new int[(int) Math.min(chunkSize, Math.max(1L, length))];
        ByteBuffer scratch = ByteBuffer.allocate(chunk.length * BYTES);
        java.util.Arrays.fill(chunk, value);
        for (long start = 0; start < length; start += chunkSize) {
            int len = chunkLength(start);
            writeIntChunk(start, chunk, len, scratch);
        }
        flush();
    }

    public int getInt(long id) throws IOException {
        validateRange(id, 1);
        ensureCachedChunk(id);
        return cachedChunk[(int) (id - cachedChunkStart)];
    }

    public void incrementInt(long id) throws IOException {
        ensureWritable();
        validateRange(id, 1);
        ensureCachedChunk(id);
        cachedChunk[(int) (id - cachedChunkStart)]++;
        cacheDirty = true;
    }

    public void flush() throws IOException {
        flushCachedChunk();
        if (writable) {
            channel.force(false);
        }
    }

    public Path path() {
        return path;
    }

    public long length() {
        return length;
    }

    private void ensureCachedChunk(long id) throws IOException {
        long chunkStart = (id / chunkSize) * (long) chunkSize;
        if (chunkStart == cachedChunkStart) {
            return;
        }
        flushCachedChunk();
        cachedChunkStart = chunkStart;
        cachedChunkLength = chunkLength(chunkStart);
        cachedChunk = readIntChunk(chunkStart, cachedChunkLength);
        cacheDirty = false;
    }

    private void flushCachedChunk() throws IOException {
        if (cacheDirty && cachedChunk != null) {
            ensureWritable();
            writeIntChunk(cachedChunkStart, cachedChunk, cachedChunkLength);
            cacheDirty = false;
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

    private synchronized void writeFully(ByteBuffer buffer, long position) throws IOException {
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
        flushCachedChunk();
        if (forceOnClose) {
            flush();
        }
        channel.close();
    }
}
