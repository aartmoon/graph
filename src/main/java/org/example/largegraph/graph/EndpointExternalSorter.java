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
import java.util.List;
import java.util.PriorityQueue;

final class EndpointExternalSorter {
    private static final int MAX_MERGE_FAN_IN = 128;
    private static final int INSERTION_SORT_THRESHOLD = 24;

    private final int maxRecordsPerRun;

    enum Order { BY_ORIGINAL_VERTEX, BY_EDGE_AND_SIDE }

    private final Order order;

    EndpointExternalSorter(int maxRecordsPerRun, Order order) {
        this.maxRecordsPerRun = Math.max(1, maxRecordsPerRun);
        this.order = order;
    }

    void sort(Path input, Path output, Path tempDir, String runPrefix) throws IOException {
        FileUtils.deleteRecursively(tempDir);
        Files.createDirectories(tempDir);
        List<Path> runs = createSortedRuns(input, tempDir, runPrefix);
        runs = compactRuns(runs, tempDir, runPrefix);
        merge(runs, output);
        FileUtils.deleteRecursively(tempDir);
    }

    private List<Path> createSortedRuns(Path input, Path tempDir, String runPrefix) throws IOException {
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
                    for (int i = 0; i < count; i++) {
                        writer.write(chunk.vertexIds[i], chunk.edgeIds[i], chunk.sides[i]);
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

        PriorityQueue<Cursor> queue = new PriorityQueue<>((left, right) -> {
            int compared = compare(left.vertexId, left.edgeId, left.side,
                    right.vertexId, right.edgeId, right.side);
            return compared != 0 ? compared : Integer.compare(left.runId, right.runId);
        });
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
                writer.write(cursor.vertexId, cursor.edgeId, cursor.side);
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
            int pivotOriginalId = chunk.vertexIds[pivotIndex];
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
            int originalId = chunk.vertexIds[i];
            long edgeId = chunk.edgeIds[i];
            byte side = chunk.sides[i];
            int j = i - 1;
            while (j >= low && compare(chunk, j, originalId, edgeId, side) > 0) {
                chunk.vertexIds[j + 1] = chunk.vertexIds[j];
                chunk.edgeIds[j + 1] = chunk.edgeIds[j];
                chunk.sides[j + 1] = chunk.sides[j];
                j--;
            }
            chunk.vertexIds[j + 1] = originalId;
            chunk.edgeIds[j + 1] = edgeId;
            chunk.sides[j + 1] = side;
        }
    }

    private int compare(Chunk chunk, int index, int originalId, long edgeId, byte side) {
        return compare(chunk.vertexIds[index], chunk.edgeIds[index], chunk.sides[index], originalId, edgeId, side);
    }

    private int compare(int leftVertex, long leftEdge, byte leftSide,
                        int rightVertex, long rightEdge, byte rightSide) {
        int compared;
        if (order == Order.BY_ORIGINAL_VERTEX) {
            compared = Integer.compare(leftVertex, rightVertex);
            if (compared == 0) compared = Long.compare(leftEdge, rightEdge);
            if (compared == 0) compared = Byte.compare(leftSide, rightSide);
        } else {
            compared = Long.compare(leftEdge, rightEdge);
            if (compared == 0) compared = Byte.compare(leftSide, rightSide);
            if (compared == 0) compared = Integer.compare(leftVertex, rightVertex);
        }
        return compared;
    }

    private void swap(Chunk chunk, int left, int right) {
        if (left == right) {
            return;
        }
        int originalId = chunk.vertexIds[left];
        chunk.vertexIds[left] = chunk.vertexIds[right];
        chunk.vertexIds[right] = originalId;

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

    private static final class Chunk {
        private final int[] vertexIds;
        private final long[] edgeIds;
        private final byte[] sides;

        private Chunk(int capacity) {
            this.vertexIds = new int[capacity];
            this.edgeIds = new long[capacity];
            this.sides = new byte[capacity];
        }
    }

    private static final class Cursor {
        private final int runId;
        private int vertexId;
        private long edgeId;
        private byte side;

        private Cursor(int runId) {
            this.runId = runId;
        }

        private int runId() {
            return runId;
        }

    }

    private final class Reader implements Closeable {
        private final DataInputStream input;

        private Reader(Path path) throws IOException {
            validateRecordAlignment(path, Integer.BYTES + Long.BYTES + Byte.BYTES);
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        private int readChunk(Chunk chunk) throws IOException {
            int count = 0;
            while (count < chunk.vertexIds.length) {
                try {
                    read(chunk, count);
                    count++;
                } catch (EOFException ex) {
                    break;
                }
            }
            return count;
        }

        private boolean read(Cursor cursor) throws IOException {
            try {
                if (order == Order.BY_ORIGINAL_VERTEX) {
                    cursor.vertexId = input.readInt();
                    cursor.edgeId = input.readLong();
                    cursor.side = input.readByte();
                } else {
                    cursor.edgeId = input.readLong();
                    cursor.side = input.readByte();
                    cursor.vertexId = input.readInt();
                }
                return true;
            } catch (EOFException ex) {
                return false;
            }
        }

        private void read(Chunk chunk, int index) throws IOException {
            if (order == Order.BY_ORIGINAL_VERTEX) {
                chunk.vertexIds[index] = input.readInt();
                chunk.edgeIds[index] = input.readLong();
                chunk.sides[index] = input.readByte();
            } else {
                chunk.edgeIds[index] = input.readLong();
                chunk.sides[index] = input.readByte();
                chunk.vertexIds[index] = input.readInt();
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private final class Writer implements Closeable {
        private final DataOutputStream output;

        private Writer(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        private void write(int vertexId, long edgeId, byte side) throws IOException {
            if (order == Order.BY_ORIGINAL_VERTEX) {
                output.writeInt(vertexId);
                output.writeLong(edgeId);
                output.writeByte(side);
            } else {
                output.writeLong(edgeId);
                output.writeByte(side);
                output.writeInt(vertexId);
            }
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
