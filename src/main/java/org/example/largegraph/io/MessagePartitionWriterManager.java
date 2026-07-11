package org.example.largegraph.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public final class MessagePartitionWriterManager implements Closeable {
    private final Path workerDir;
    private final MessageBucketLayout bucketLayout;
    private final int maxOpenWriters;
    private final Map<Integer, BinaryMessageWriter> writers;
    private final TreeSet<Integer> touchedBuckets;

    public MessagePartitionWriterManager(
            Path workerDir,
            int destinationPartitionCount,
            int chunkSize,
            long vertexCount,
            int maxOpenWriters
    ) throws IOException {
        this.workerDir = workerDir;
        this.bucketLayout = new MessageBucketLayout(destinationPartitionCount, chunkSize, vertexCount);
        this.maxOpenWriters = Math.max(1, maxOpenWriters);
        this.writers = new LinkedHashMap<>(16, 0.75f, true);
        this.touchedBuckets = new TreeSet<>();
        Files.createDirectories(workerDir);
    }

    public void write(int destinationPartition, int to, double contribution) throws IOException {
        int bucket = bucketLayout.bucketFor(destinationPartition, to);
        touchedBuckets.add(bucket);
        writerFor(bucket).write(to, contribution);
    }

    public Path pathFor(int bucket) {
        return workerDir.resolve("msg-bucket-%05d.bin".formatted(bucket));
    }

    private BinaryMessageWriter writerFor(int bucket) throws IOException {
        BinaryMessageWriter writer = writers.get(bucket);
        if (writer != null) {
            return writer;
        }
        evictIfNeeded();
        writer = new BinaryMessageWriter(pathFor(bucket));
        writers.put(bucket, writer);
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
    }

    public List<MessageFileInfo> messageFiles() throws IOException {
        List<MessageFileInfo> files = new ArrayList<>(touchedBuckets.size());
        for (int bucket : touchedBuckets) {
            Path path = pathFor(bucket);
            files.add(new MessageFileInfo(bucket, path, Files.size(path)));
        }
        return List.copyOf(files);
    }

    public record MessageFileInfo(int bucket, Path path, long bytes) {
    }
}
