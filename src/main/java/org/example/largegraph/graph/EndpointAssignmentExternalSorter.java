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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

final class EndpointAssignmentExternalSorter {
    private static final int MAX_MERGE_FAN_IN = 128;
    private static final int INSERTION_SORT_THRESHOLD = 24;

    private final int maxRecordsPerRun;

    EndpointAssignmentExternalSorter(int maxRecordsPerRun) {
        this.maxRecordsPerRun = Math.max(1, maxRecordsPerRun);
    }

    void sort(Path input, Path output, Path tempDir, String runPrefix) throws IOException {
        deleteRecursively(tempDir);
        Files.createDirectories(tempDir);
        List<Path> runs = createSortedRuns(input, tempDir, runPrefix);
        runs = compactRuns(runs, tempDir, runPrefix);
        merge(runs, output);
        deleteRecursively(tempDir);
    }

    private List<Path> createSortedRuns(Path input, Path tempDir, String runPrefix) throws IOException {
        List<Path> runs = new ArrayList<>();
        try (Reader reader = new Reader(input)) {
            int runId = 0;
            while (true) {
                Chunk chunk = new Chunk(maxRecordsPerRun);
                int count = reader.readChunk(chunk);
                if (count == 0) {
                    break;
                }
                sortChunk(chunk, count);
                Path run = tempDir.resolve("%s-%05d.bin".formatted(runPrefix, runId++));
                try (Writer writer = new Writer(run)) {
                    for (int i = 0; i < count; i++) {
                        writer.write(chunk.edgeIds[i], chunk.sides[i], chunk.denseIds[i]);
                    }
                }
                addRunBounded(runs, run, tempDir, runPrefix);
            }
        }
        return runs;
    }

    private void addRunBounded(List<Path> runs, Path run, Path tempDir, String runPrefix) throws IOException {
        runs.add(run);
        if (runs.size() <= MAX_MERGE_FAN_IN) {
            return;
        }
        List<Path> group = new ArrayList<>(runs.subList(0, MAX_MERGE_FAN_IN));
        Path merged = tempDir.resolve("%s-incremental-merged-%05d.bin".formatted(runPrefix, System.nanoTime()));
        merge(group, merged);
        for (Path oldRun : group) {
            Files.deleteIfExists(oldRun);
        }
        runs.subList(0, MAX_MERGE_FAN_IN).clear();
        runs.add(merged);
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

    private void merge(List<Path> runs, Path output) throws IOException {
        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }

        PriorityQueue<Cursor> queue = new PriorityQueue<>(Comparator
                .comparingLong(Cursor::edgeId)
                .thenComparingInt(Cursor::side)
                .thenComparingInt(Cursor::runId));
        List<Reader> readers = new ArrayList<>(runs.size());
        IOException failure = null;

        try (Writer writer = new Writer(output)) {
            for (int i = 0; i < runs.size(); i++) {
                Reader reader = new Reader(runs.get(i));
                readers.add(reader);
                Cursor cursor = new Cursor(i);
                if (reader.read(cursor)) {
                    queue.add(cursor);
                }
            }

            while (!queue.isEmpty()) {
                Cursor cursor = queue.poll();
                writer.write(cursor.edgeId(), cursor.side(), cursor.denseId());
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
        quickSort(chunk, 0, count - 1);
        insertionSort(chunk, 0, count - 1);
    }

    private void quickSort(Chunk chunk, int low, int high) {
        while (high - low > INSERTION_SORT_THRESHOLD) {
            int pivotIndex = low + ((high - low) >>> 1);
            long pivotEdgeId = chunk.edgeIds[pivotIndex];
            byte pivotSide = chunk.sides[pivotIndex];
            int i = low;
            int j = high;
            while (i <= j) {
                while (compare(chunk, i, pivotEdgeId, pivotSide) < 0) {
                    i++;
                }
                while (compare(chunk, j, pivotEdgeId, pivotSide) > 0) {
                    j--;
                }
                if (i <= j) {
                    swap(chunk, i, j);
                    i++;
                    j--;
                }
            }
            if (j - low < high - i) {
                if (low < j) {
                    quickSort(chunk, low, j);
                }
                low = i;
            } else {
                if (i < high) {
                    quickSort(chunk, i, high);
                }
                high = j;
            }
        }
    }

    private void insertionSort(Chunk chunk, int low, int high) {
        for (int i = low + 1; i <= high; i++) {
            long edgeId = chunk.edgeIds[i];
            byte side = chunk.sides[i];
            int denseId = chunk.denseIds[i];
            int j = i - 1;
            while (j >= low && compare(chunk, j, edgeId, side) > 0) {
                chunk.edgeIds[j + 1] = chunk.edgeIds[j];
                chunk.sides[j + 1] = chunk.sides[j];
                chunk.denseIds[j + 1] = chunk.denseIds[j];
                j--;
            }
            chunk.edgeIds[j + 1] = edgeId;
            chunk.sides[j + 1] = side;
            chunk.denseIds[j + 1] = denseId;
        }
    }

    private int compare(Chunk chunk, int index, long edgeId, byte side) {
        int compared = Long.compare(chunk.edgeIds[index], edgeId);
        if (compared != 0) {
            return compared;
        }
        return Byte.compare(chunk.sides[index], side);
    }

    private void swap(Chunk chunk, int left, int right) {
        if (left == right) {
            return;
        }
        long edgeId = chunk.edgeIds[left];
        chunk.edgeIds[left] = chunk.edgeIds[right];
        chunk.edgeIds[right] = edgeId;

        byte side = chunk.sides[left];
        chunk.sides[left] = chunk.sides[right];
        chunk.sides[right] = side;

        int denseId = chunk.denseIds[left];
        chunk.denseIds[left] = chunk.denseIds[right];
        chunk.denseIds[right] = denseId;
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

    private void deleteRecursively(Path path) throws IOException {
        FileUtils.deleteRecursively(path);
    }

    private static final class Chunk {
        private final long[] edgeIds;
        private final byte[] sides;
        private final int[] denseIds;

        private Chunk(int capacity) {
            this.edgeIds = new long[capacity];
            this.sides = new byte[capacity];
            this.denseIds = new int[capacity];
        }
    }

    private static final class Cursor {
        private final int runId;
        private long edgeId;
        private byte side;
        private int denseId;

        private Cursor(int runId) {
            this.runId = runId;
        }

        private int runId() {
            return runId;
        }

        private long edgeId() {
            return edgeId;
        }

        private int side() {
            return side;
        }

        private int denseId() {
            return denseId;
        }
    }

    private static final class Reader implements Closeable {
        private final DataInputStream input;

        private Reader(Path path) throws IOException {
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        private int readChunk(Chunk chunk) throws IOException {
            int count = 0;
            while (count < chunk.edgeIds.length) {
                try {
                    chunk.edgeIds[count] = input.readLong();
                    chunk.sides[count] = input.readByte();
                    chunk.denseIds[count] = input.readInt();
                    count++;
                } catch (EOFException ex) {
                    break;
                }
            }
            return count;
        }

        private boolean read(Cursor cursor) throws IOException {
            try {
                cursor.edgeId = input.readLong();
                cursor.side = input.readByte();
                cursor.denseId = input.readInt();
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

        private void write(long edgeId, int side, int denseId) throws IOException {
            output.writeLong(edgeId);
            output.writeByte(side);
            output.writeInt(denseId);
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }
}
