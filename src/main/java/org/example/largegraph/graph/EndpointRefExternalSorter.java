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

final class EndpointRefExternalSorter {
    private static final int MAX_MERGE_FAN_IN = 128;
    private static final int INSERTION_SORT_THRESHOLD = 24;

    private final int maxRecordsPerRun;

    EndpointRefExternalSorter(int maxRecordsPerRun) {
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
                        writer.write(chunk.originalIds[i], chunk.edgeIds[i], chunk.sides[i]);
                    }
                }
                runs.add(run);
            }
        }
        return runs;
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
                .comparingInt(Cursor::originalId)
                .thenComparingLong(Cursor::edgeId)
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
                writer.write(cursor.originalId(), cursor.edgeId(), cursor.side());
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
            int pivotOriginalId = chunk.originalIds[pivotIndex];
            long pivotEdgeId = chunk.edgeIds[pivotIndex];
            byte pivotSide = chunk.sides[pivotIndex];
            int i = low;
            int j = high;
            while (i <= j) {
                while (compare(chunk, i, pivotOriginalId, pivotEdgeId, pivotSide) < 0) {
                    i++;
                }
                while (compare(chunk, j, pivotOriginalId, pivotEdgeId, pivotSide) > 0) {
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
            int originalId = chunk.originalIds[i];
            long edgeId = chunk.edgeIds[i];
            byte side = chunk.sides[i];
            int j = i - 1;
            while (j >= low && compare(chunk, j, originalId, edgeId, side) > 0) {
                chunk.originalIds[j + 1] = chunk.originalIds[j];
                chunk.edgeIds[j + 1] = chunk.edgeIds[j];
                chunk.sides[j + 1] = chunk.sides[j];
                j--;
            }
            chunk.originalIds[j + 1] = originalId;
            chunk.edgeIds[j + 1] = edgeId;
            chunk.sides[j + 1] = side;
        }
    }

    private int compare(Chunk chunk, int index, int originalId, long edgeId, byte side) {
        int compared = Integer.compare(chunk.originalIds[index], originalId);
        if (compared != 0) {
            return compared;
        }
        compared = Long.compare(chunk.edgeIds[index], edgeId);
        if (compared != 0) {
            return compared;
        }
        return Byte.compare(chunk.sides[index], side);
    }

    private void swap(Chunk chunk, int left, int right) {
        if (left == right) {
            return;
        }
        int originalId = chunk.originalIds[left];
        chunk.originalIds[left] = chunk.originalIds[right];
        chunk.originalIds[right] = originalId;

        long edgeId = chunk.edgeIds[left];
        chunk.edgeIds[left] = chunk.edgeIds[right];
        chunk.edgeIds[right] = edgeId;

        byte side = chunk.sides[left];
        chunk.sides[left] = chunk.sides[right];
        chunk.sides[right] = side;
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
        private final int[] originalIds;
        private final long[] edgeIds;
        private final byte[] sides;

        private Chunk(int capacity) {
            this.originalIds = new int[capacity];
            this.edgeIds = new long[capacity];
            this.sides = new byte[capacity];
        }
    }

    private static final class Cursor {
        private final int runId;
        private int originalId;
        private long edgeId;
        private byte side;

        private Cursor(int runId) {
            this.runId = runId;
        }

        private int runId() {
            return runId;
        }

        private int originalId() {
            return originalId;
        }

        private long edgeId() {
            return edgeId;
        }

        private byte side() {
            return side;
        }
    }

    private static final class Reader implements Closeable {
        private final DataInputStream input;

        private Reader(Path path) throws IOException {
            validateRecordAlignment(path, Integer.BYTES + Long.BYTES + Byte.BYTES);
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        private int readChunk(Chunk chunk) throws IOException {
            int count = 0;
            while (count < chunk.originalIds.length) {
                try {
                    chunk.originalIds[count] = input.readInt();
                    chunk.edgeIds[count] = input.readLong();
                    chunk.sides[count] = input.readByte();
                    count++;
                } catch (EOFException ex) {
                    break;
                }
            }
            return count;
        }

        private boolean read(Cursor cursor) throws IOException {
            try {
                cursor.originalId = input.readInt();
                cursor.edgeId = input.readLong();
                cursor.side = input.readByte();
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

        private void write(int originalId, long edgeId, byte side) throws IOException {
            output.writeInt(originalId);
            output.writeLong(edgeId);
            output.writeByte(side);
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
}
