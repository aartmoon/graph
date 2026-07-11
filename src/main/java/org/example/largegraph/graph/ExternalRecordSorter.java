package org.example.largegraph.graph;

import org.example.largegraph.util.FileUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;

public final class ExternalRecordSorter<T> {
    private static final int MAX_MERGE_FAN_IN = 128;

    private final RecordReaderFactory<T> readerFactory;
    private final RecordWriterFactory<T> writerFactory;
    private final Comparator<T> comparator;
    private final int maxRecordsPerRun;

    public ExternalRecordSorter(
            RecordReaderFactory<T> readerFactory,
            RecordWriterFactory<T> writerFactory,
            Comparator<T> comparator,
            int maxRecordsPerRun
    ) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.comparator = comparator;
        this.maxRecordsPerRun = Math.max(1, maxRecordsPerRun);
    }

    public void sort(Path input, Path output, Path tempDir, String runPrefix) throws IOException {
        deleteRecursively(tempDir);
        Files.createDirectories(tempDir);
        List<Path> runs = createSortedRuns(input, tempDir, runPrefix);
        runs = compactRuns(runs, tempDir, runPrefix);
        merge(runs, output);
        deleteRecursively(tempDir);
    }

    private List<Path> compactRuns(List<Path> runs, Path tempDir, String runPrefix) throws IOException {
        List<Path> current = runs;
        int pass = 0;
        while (current.size() > MAX_MERGE_FAN_IN) {
            Path passDir = tempDir.resolve("merge-pass-%02d".formatted(pass++));
            Files.createDirectories(passDir);
            List<Path> next = new ArrayList<>((current.size() + MAX_MERGE_FAN_IN - 1) / MAX_MERGE_FAN_IN);

            for (int start = 0; start < current.size(); start += MAX_MERGE_FAN_IN) {
                int end = Math.min(current.size(), start + MAX_MERGE_FAN_IN);
                List<Path> group = current.subList(start, end);
                Path merged = passDir.resolve("%s-merged-%05d.bin".formatted(runPrefix, next.size()));
                merge(group, merged);
                next.add(merged);
                for (Path run : group) {
                    Files.deleteIfExists(run);
                }
            }
            current = next;
        }
        return current;
    }

    private List<Path> createSortedRuns(Path input, Path tempDir, String runPrefix) throws IOException {
        List<Path> runs = new ArrayList<>();
        try (RecordReader<T> reader = readerFactory.open(input)) {
            int runId = 0;
            while (true) {
                List<T> chunk = new ArrayList<>(maxRecordsPerRun);
                Optional<T> record;
                while (chunk.size() < maxRecordsPerRun && (record = reader.next()).isPresent()) {
                    chunk.add(record.get());
                }
                if (chunk.isEmpty()) {
                    break;
                }
                chunk.sort(comparator);
                Path run = tempDir.resolve("%s-%05d.bin".formatted(runPrefix, runId++));
                try (RecordWriter<T> writer = writerFactory.open(run)) {
                    for (T value : chunk) {
                        writer.write(value);
                    }
                }
                runs.add(run);
            }
        }
        return runs;
    }

    private void merge(List<Path> runs, Path output) throws IOException {
        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }

        PriorityQueue<Cursor<T>> queue = new PriorityQueue<>((left, right) -> {
            int compared = comparator.compare(left.value(), right.value());
            if (compared != 0) {
                return compared;
            }
            return Integer.compare(left.runId(), right.runId());
        });
        List<RecordReader<T>> readers = new ArrayList<>(runs.size());
        IOException failure = null;

        try (RecordWriter<T> writer = writerFactory.open(output)) {
            for (int i = 0; i < runs.size(); i++) {
                int runId = i;
                RecordReader<T> reader = readerFactory.open(runs.get(i));
                readers.add(reader);
                Optional<T> value = reader.next();
                value.ifPresent(record -> queue.add(new Cursor<>(runId, record)));
            }

            while (!queue.isEmpty()) {
                Cursor<T> cursor = queue.poll();
                writer.write(cursor.value());

                Optional<T> next = readers.get(cursor.runId()).next();
                next.ifPresent(record -> queue.add(new Cursor<>(cursor.runId(), record)));
            }
        } catch (IOException ex) {
            failure = ex;
            throw ex;
        } finally {
            for (RecordReader<T> reader : readers) {
                try {
                    reader.close();
                } catch (IOException ex) {
                    if (failure != null) {
                        failure.addSuppressed(ex);
                    } else {
                        throw ex;
                    }
                }
            }
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        FileUtils.deleteRecursively(path);
    }

    private record Cursor<T>(int runId, T value) {
    }

    @FunctionalInterface
    public interface RecordReaderFactory<T> {
        RecordReader<T> open(Path path) throws IOException;
    }

    @FunctionalInterface
    public interface RecordWriterFactory<T> {
        RecordWriter<T> open(Path path) throws IOException;
    }

    public interface RecordReader<T> extends Closeable {
        Optional<T> next() throws IOException;
    }

    public interface RecordWriter<T> extends Closeable {
        void write(T record) throws IOException;
    }
}
