package org.example.largegraph.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

public final class MessagePartitionWriterManager implements Closeable {
    private final Path workerDir;
    private final int destinationPartitionCount;
    private final int maxOpenWriters;
    private final Map<Integer, BinaryMessageWriter> writers;
    private final TreeSet<Integer> touchedPartitions;

    public MessagePartitionWriterManager(Path workerDir, int destinationPartitionCount, int maxOpenWriters) throws IOException {
        this.workerDir = workerDir;
        this.destinationPartitionCount = destinationPartitionCount;
        this.maxOpenWriters = Math.max(1, maxOpenWriters);
        this.writers = new LinkedHashMap<>(16, 0.75f, true);
        this.touchedPartitions = new TreeSet<>();
        Files.createDirectories(workerDir);
    }

    public void write(int destinationPartition, int to, double contribution) throws IOException {
        if (destinationPartition < 0 || destinationPartition >= destinationPartitionCount) {
            throw new IllegalArgumentException("invalid destination partition: " + destinationPartition);
        }
        touchedPartitions.add(destinationPartition);
        writerFor(destinationPartition).write(to, contribution);
    }

    public Path pathFor(int destinationPartition) {
        return workerDir.resolve("msg-part-%05d.bin".formatted(destinationPartition));
    }

    private BinaryMessageWriter writerFor(int partition) throws IOException {
        BinaryMessageWriter writer = writers.get(partition);
        if (writer != null) {
            return writer;
        }
        evictIfNeeded();
        writer = new BinaryMessageWriter(pathFor(partition));
        writers.put(partition, writer);
        return writer;
    }

    private void evictIfNeeded() throws IOException {
        if (writers.size() < maxOpenWriters) {
            return;
        }
        Iterator<Map.Entry<Integer, BinaryMessageWriter>> iterator = writers.entrySet().iterator();
        if (iterator.hasNext()) {
            Map.Entry<Integer, BinaryMessageWriter> eldest = iterator.next();
            eldest.getValue().close();
            iterator.remove();
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (BinaryMessageWriter writer : writers.values()) {
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
        writeManifest();
    }

    private void writeManifest() throws IOException {
        Path manifest = workerDir.resolve("manifest.txt");
        try (var writer = Files.newBufferedWriter(manifest, StandardCharsets.UTF_8)) {
            for (int partition : touchedPartitions) {
                writer.write(Integer.toString(partition));
                writer.newLine();
            }
        }
    }
}
