package org.example.largegraph.graph;

import org.example.largegraph.util.FileUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

final class DenseEdgeExternalSorter {
    private static final int MAX_MERGE_FAN_IN = 128;

    private final int maxRecordsPerRun;

    DenseEdgeExternalSorter(int maxRecordsPerRun) {
        this.maxRecordsPerRun = Math.max(1, maxRecordsPerRun);
    }

    void sortUnique(Path input, Path output, Path tempDir, String runPrefix) throws IOException {
        FileUtils.deleteRecursively(tempDir);
        Files.createDirectories(tempDir);
        List<Path> runs = createSortedUniqueRuns(input, tempDir, runPrefix);
        runs = compactRuns(runs, tempDir, runPrefix);
        mergeUnique(runs, output);
        FileUtils.deleteRecursively(tempDir);
    }

    void sortUniqueToSink(Path input, Path tempDir, String runPrefix, EdgeSink sink) throws IOException {
        FileUtils.deleteRecursively(tempDir);
        Files.createDirectories(tempDir);
        List<Path> runs = createSortedUniqueRuns(input, tempDir, runPrefix);
        runs = compactRuns(runs, tempDir, runPrefix);
        mergeUniqueToSink(runs, sink);
        for (Path run : runs) {
            Files.deleteIfExists(run);
        }
        FileUtils.deleteRecursively(tempDir);
    }

    private List<Path> createSortedUniqueRuns(Path input, Path tempDir, String runPrefix) throws IOException {
        List<Path> runs = new ArrayList<>();
        try (Reader reader = new Reader(input)) {
            Chunk chunk = new Chunk(maxRecordsPerRun);
            int runId = 0;
            while (true) {
                int count = reader.readChunk(chunk);
                if (count == 0) {
                    break;
                }
                sortChunk(chunk, count);
                Path run = tempDir.resolve("%s-%05d.bin".formatted(runPrefix, runId++));
                try (Writer writer = new Writer(run)) {
                    writeUniqueChunk(chunk, count, writer);
                }
                runs.add(run);
            }
        }
        return runs;
    }

    private void writeUniqueChunk(Chunk chunk, int count, Writer writer) throws IOException {
        boolean haveLast = false;
        long last = 0;
        for (int i = 0; i < count; i++) {
            long edge = chunk.edges[i];
            if (!haveLast || edge != last) {
                writer.write((int) (edge >>> 32), (int) edge);
                last = edge;
                haveLast = true;
            }
        }
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
                mergeUnique(group, merged);
                next.add(merged);
                for (Path run : group) {
                    Files.deleteIfExists(run);
                }
            }
            current = next;
        }
        return current;
    }

    private void mergeUnique(List<Path> runs, Path output) throws IOException {
        try (Writer writer = new Writer(output)) {
            mergeUniqueToSink(runs, writer::write);
        }
    }

    private void mergeUniqueToSink(List<Path> runs, EdgeSink sink) throws IOException {
        PriorityQueue<Cursor> queue = new PriorityQueue<>(Comparator
                .comparingInt(Cursor::from)
                .thenComparingInt(Cursor::to)
                .thenComparingInt(Cursor::runId));
        List<Reader> readers = new ArrayList<>(runs.size());
        IOException failure = null;

        try {
            for (int i = 0; i < runs.size(); i++) {
                Reader reader = new Reader(runs.get(i));
                readers.add(reader);
                Cursor cursor = new Cursor(i);
                if (reader.read(cursor)) {
                    queue.add(cursor);
                }
            }

            boolean haveLast = false;
            int lastFrom = 0;
            int lastTo = 0;
            while (!queue.isEmpty()) {
                Cursor cursor = queue.poll();
                if (!haveLast || cursor.from() != lastFrom || cursor.to() != lastTo) {
                    sink.accept(cursor.from(), cursor.to());
                    lastFrom = cursor.from();
                    lastTo = cursor.to();
                    haveLast = true;
                }
                if (readers.get(cursor.runId()).read(cursor)) {
                    queue.add(cursor);
                }
            }
        } catch (IOException ex) {
            failure = ex;
            throw ex;
        } finally {
            closeReaders(readers, failure);
        }
    }

    private void sortChunk(Chunk chunk, int count) {
        Arrays.sort(chunk.edges, 0, count);
    }

    private void closeReaders(List<Reader> readers, IOException failure) throws IOException {
        for (Reader reader : readers) {
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

    private static final class Chunk {
        private final long[] edges;

        private Chunk(int capacity) {
            this.edges = new long[capacity];
        }
    }

    private static final class Cursor {
        private final int runId;
        private int from;
        private int to;

        private Cursor(int runId) {
            this.runId = runId;
        }

        private int runId() {
            return runId;
        }

        private int from() {
            return from;
        }

        private int to() {
            return to;
        }
    }

    private static final class Reader implements Closeable {
        private final DataInputStream input;

        private Reader(Path path) throws IOException {
            validateRecordAlignment(path, Integer.BYTES * 2);
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        private int readChunk(Chunk chunk) throws IOException {
            int count = 0;
            while (count < chunk.edges.length) {
                try {
                    int from = input.readInt();
                    int to = input.readInt();
                    if (from < 0 || to < 0) {
                        throw new IOException("negative dense vertex id in " + input);
                    }
                    chunk.edges[count] = ((long) from << 32) | (to & 0xffff_ffffL);
                    count++;
                } catch (EOFException ex) {
                    break;
                }
            }
            return count;
        }

        private boolean read(Cursor cursor) throws IOException {
            try {
                cursor.from = input.readInt();
                cursor.to = input.readInt();
                return true;
            } catch (EOFException ex) {
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class Writer implements Closeable {
        private final DataOutputStream output;

        private Writer(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        private void write(int from, int to) throws IOException {
            output.writeInt(from);
            output.writeInt(to);
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

    private static void validateRecordAlignment(Path path, int recordBytes) throws IOException {
        long size = Files.size(path);
        if (size % recordBytes != 0) {
            throw new IOException("corrupted file: %s, size=%d, recordBytes=%d"
                    .formatted(path, size, recordBytes));
        }
    }

    @FunctionalInterface
    interface EdgeSink {
        void accept(int from, int to) throws IOException;
    }
}
