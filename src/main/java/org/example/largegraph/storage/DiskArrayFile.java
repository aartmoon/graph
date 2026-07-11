package org.example.largegraph.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

final class DiskArrayFile implements Closeable {
    private final Path path;
    private final long length;
    private final int elementBytes;
    private final boolean writable;
    private final boolean forceOnClose;
    private final FileChannel channel;

    DiskArrayFile(Path path, long length, int elementBytes, boolean writable, boolean forceOnClose) throws IOException {
        this.path = path;
        this.length = length;
        this.elementBytes = elementBytes;
        this.writable = writable;
        this.forceOnClose = forceOnClose;
        if (writable && path.getParent() != null) Files.createDirectories(path.getParent());
        channel = writable
                ? FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
                : FileChannel.open(path, StandardOpenOption.READ);
    }

    ByteBuffer prepare(ByteBuffer scratch, long start, int count) {
        validateRange(start, count);
        int bytes = Math.multiplyExact(count, elementBytes);
        if (scratch.capacity() < bytes) {
            throw new IllegalArgumentException("scratch buffer is too small: required=%d capacity=%d"
                    .formatted(bytes, scratch.capacity()));
        }
        scratch.clear().limit(bytes);
        return scratch;
    }

    void validateRange(long start, int count) {
        if (start < 0 || count < 0 || start > length - count) {
            throw new IllegalArgumentException("invalid disk array range: start=%d length=%d arrayLength=%d"
                    .formatted(start, count, length));
        }
    }

    static void validateArrayRange(int arrayLength, int offset, int count) {
        if (offset < 0 || count < 0 || offset > arrayLength - count) {
            throw new IllegalArgumentException("invalid target range: offset=%d length=%d targetLength=%d"
                    .formatted(offset, count, arrayLength));
        }
    }

    void ensureWritable() {
        if (!writable) throw new IllegalStateException("disk array is opened read-only: " + path);
    }

    void truncate() throws IOException { ensureWritable(); channel.truncate(Math.multiplyExact(length, elementBytes)); }

    void readFully(ByteBuffer buffer, long elementOffset) throws IOException {
        transfer(buffer, Math.multiplyExact(elementOffset, elementBytes), false);
    }

    void writeFully(ByteBuffer buffer, long elementOffset) throws IOException {
        ensureWritable();
        transfer(buffer, Math.multiplyExact(elementOffset, elementBytes), true);
    }

    private void transfer(ByteBuffer buffer, long position, boolean write) throws IOException {
        while (buffer.hasRemaining()) {
            int transferred = write ? channel.write(buffer, position) : channel.read(buffer, position);
            if (transferred < 0) throw new IOException("unexpected EOF while reading " + path);
            if (transferred == 0) throw new IOException("zero-byte " + (write ? "write" : "read") + " on " + path);
            position += transferred;
        }
    }

    void force() throws IOException { if (writable) channel.force(false); }

    @Override public void close() throws IOException {
        if (forceOnClose) force();
        channel.close();
    }
}
