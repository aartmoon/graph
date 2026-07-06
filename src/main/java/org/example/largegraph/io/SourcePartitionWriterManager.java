package org.example.largegraph.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class SourcePartitionWriterManager implements Closeable {
    private final Path edgesDir;
    private final int partitionCount;
    private final int maxOpenWriters;
    private final Map<Integer, BinaryEdgeWriter> writers;

    public SourcePartitionWriterManager(Path workDir, int partitionCount, int maxOpenWriters) throws IOException {
        this.edgesDir = workDir.resolve("edges_by_source");
        this.partitionCount = partitionCount;
        this.maxOpenWriters = Math.max(1, maxOpenWriters);
        this.writers = new LinkedHashMap<>(16, 0.75f, true);
        Files.createDirectories(edgesDir);
    }

    public void write(int partition, int from, int to) throws IOException {
        if (partition < 0 || partition >= partitionCount) {
            throw new IllegalArgumentException("invalid source partition: " + partition);
        }
        writerFor(partition).write(from, to);
    }

    public Path pathFor(int partition) {
        return edgesDir.resolve("src-part-%05d.bin".formatted(partition));
    }

    private BinaryEdgeWriter writerFor(int partition) throws IOException {
        BinaryEdgeWriter writer = writers.get(partition);
        if (writer != null) {
            return writer;
        }
        evictIfNeeded();
        writer = new BinaryEdgeWriter(pathFor(partition));
        writers.put(partition, writer);
        return writer;
    }

    private void evictIfNeeded() throws IOException {
        if (writers.size() < maxOpenWriters) {
            return;
        }
        Iterator<Map.Entry<Integer, BinaryEdgeWriter>> iterator = writers.entrySet().iterator();
        if (iterator.hasNext()) {
            Map.Entry<Integer, BinaryEdgeWriter> eldest = iterator.next();
            eldest.getValue().close();
            iterator.remove();
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (BinaryEdgeWriter writer : writers.values()) {
            try {
                writer.close();
            } catch (IOException ex) {
                if (failure == null) {
                    failure = ex;
                } else {
                    failure.addSuppressed(ex);
                }
            }
        }
        writers.clear();
        if (failure != null) {
            throw failure;
        }
    }
}
