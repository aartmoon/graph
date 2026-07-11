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

final class DenseEdgeExternalSorter {
    private static final int MAX_MERGE_FAN_IN = 128;
    private static final int INSERTION_SORT_THRESHOLD = 24;

    private final int maxRecordsPerRun;

    DenseEdgeExternalSorter(int maxRecordsPerRun) {
        this.maxRecordsPerRun = Math.max(1, maxRecordsPerRun);
    }

    void sortUnique(Path input, Path output, Path tempDir, String runPrefix) throws IOException {
        deleteRecursively(tempDir);
        Files.createDirectories(tempDir);
        List<Path> runs = createSortedUniqueRuns(input, tempDir, runPrefix);
        runs = compactRuns(runs, tempDir, runPrefix);
        mergeUnique(runs, output);
        deleteRecursively(tempDir);
    }

    private List<Path> createSortedUniqueRuns(Path input, Path tempDir, String runPrefix) throws IOException {
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
                    writeUniqueChunk(chunk, count, writer);
                }
                runs.add(run);
            }
        }
        return runs;
    }

    private void writeUniqueChunk(Chunk chunk, int count, Writer writer) throws IOException {
        boolean haveLast = false;
        int lastFrom = 0;
        int lastTo = 0;
        for (int i = 0; i < count; i++) {
            int from = chunk.froms[i];
            int to = chunk.tos[i];
            if (!haveLast || from != lastFrom || to != lastTo) {
                writer.write(from, to);
                lastFrom = from;
                lastTo = to;
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
        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }

        PriorityQueue<Cursor> queue = new PriorityQueue<>(Comparator
                .comparingInt(Cursor::from)
                .thenComparingInt(Cursor::to)
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

            boolean haveLast = false;
            int lastFrom = 0;
            int lastTo = 0;
            while (!queue.isEmpty()) {
                Cursor cursor = queue.poll();
                if (!haveLast || cursor.from() != lastFrom || cursor.to() != lastTo) {
                    writer.write(cursor.from(), cursor.to());
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
        quickSort(chunk, 0, count - 1);
        insertionSort(chunk, 0, count - 1);
    }

    private void quickSort(Chunk chunk, int low, int high) {
        while (high - low > INSERTION_SORT_THRESHOLD) {
            int pivotIndex = low + ((high - low) >>> 1);
            int pivotFrom = chunk.froms[pivotIndex];
            int pivotTo = chunk.tos[pivotIndex];
            int i = low;
            int j = high;
            while (i <= j) {
                while (compare(chunk, i, pivotFrom, pivotTo) < 0) {
                    i++;
                }
                while (compare(chunk, j, pivotFrom, pivotTo) > 0) {
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
            int from = chunk.froms[i];
            int to = chunk.tos[i];
            int j = i - 1;
            while (j >= low && compare(chunk, j, from, to) > 0) {
                chunk.froms[j + 1] = chunk.froms[j];
                chunk.tos[j + 1] = chunk.tos[j];
                j--;
            }
            chunk.froms[j + 1] = from;
            chunk.tos[j + 1] = to;
        }
    }

    private int compare(Chunk chunk, int index, int from, int to) {
        int compared = Integer.compare(chunk.froms[index], from);
        if (compared != 0) {
            return compared;
        }
        return Integer.compare(chunk.tos[index], to);
    }

    private void swap(Chunk chunk, int left, int right) {
        if (left == right) {
            return;
        }
        int from = chunk.froms[left];
        chunk.froms[left] = chunk.froms[right];
        chunk.froms[right] = from;

        int to = chunk.tos[left];
        chunk.tos[left] = chunk.tos[right];
        chunk.tos[right] = to;
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
        private final int[] froms;
        private final int[] tos;

        private Chunk(int capacity) {
            this.froms = new int[capacity];
            this.tos = new int[capacity];
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
            while (count < chunk.froms.length) {
                try {
                    chunk.froms[count] = input.readInt();
                    chunk.tos[count] = input.readInt();
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
}
