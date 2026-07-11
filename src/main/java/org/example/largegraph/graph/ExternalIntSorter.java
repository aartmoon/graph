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
import java.util.OptionalInt;
import java.util.PriorityQueue;

public final class ExternalIntSorter {
    private static final int MAX_MERGE_FAN_IN = 128;

    private final int maxIntsPerRun;

    public ExternalIntSorter(int maxIntsPerRun) {
        this.maxIntsPerRun = Math.max(1, maxIntsPerRun);
    }

    public SortUniqueResult sortUniqueToVerticesAndMapping(
            Path unsortedInput,
            Path verticesOutput,
            Path mappingOutput,
            Path tempDir
    ) throws IOException {
        deleteRecursively(tempDir);
        Files.createDirectories(tempDir);

        List<Path> runs = createSortedRuns(unsortedInput, tempDir);
        runs = compactRuns(runs, tempDir);
        long uniqueCount = mergeUnique(runs, verticesOutput, mappingOutput);
        deleteRecursively(tempDir);
        return new SortUniqueResult(uniqueCount);
    }

    private List<Path> createSortedRuns(Path input, Path tempDir) throws IOException {
        List<Path> runs = new ArrayList<>();
        try (IntReader reader = new IntReader(input)) {
            int runId = 0;
            while (true) {
                int[] chunk = new int[maxIntsPerRun];
                int count = 0;
                OptionalInt value;
                while (count < maxIntsPerRun && (value = reader.next()).isPresent()) {
                    chunk[count++] = value.getAsInt();
                }
                if (count == 0) {
                    break;
                }
                java.util.Arrays.sort(chunk, 0, count);
                Path run = tempDir.resolve("int-run-%05d.bin".formatted(runId++));
                try (IntWriter writer = new IntWriter(run)) {
                    boolean haveLast = false;
                    int last = 0;
                    for (int i = 0; i < count; i++) {
                        if (!haveLast || chunk[i] != last) {
                            writer.write(chunk[i]);
                            last = chunk[i];
                            haveLast = true;
                        }
                    }
                }
                runs.add(run);
            }
        }
        return runs;
    }

    private List<Path> compactRuns(List<Path> runs, Path tempDir) throws IOException {
        List<Path> current = runs;
        int pass = 0;
        while (current.size() > MAX_MERGE_FAN_IN) {
            Path passDir = tempDir.resolve("merge-pass-%02d".formatted(pass++));
            Files.createDirectories(passDir);
            List<Path> next = new ArrayList<>((current.size() + MAX_MERGE_FAN_IN - 1) / MAX_MERGE_FAN_IN);

            for (int start = 0; start < current.size(); start += MAX_MERGE_FAN_IN) {
                int end = Math.min(current.size(), start + MAX_MERGE_FAN_IN);
                List<Path> group = current.subList(start, end);
                Path merged = passDir.resolve("int-merged-%05d.bin".formatted(next.size()));
                mergeSortedRuns(group, merged);
                next.add(merged);
                for (Path run : group) {
                    Files.deleteIfExists(run);
                }
            }
            current = next;
        }
        return current;
    }

    private void mergeSortedRuns(List<Path> runs, Path output) throws IOException {
        if (output.getParent() != null) {
            Files.createDirectories(output.getParent());
        }

        PriorityQueue<IntCursor> queue = new PriorityQueue<>(Comparator
                .comparingInt(IntCursor::value)
                .thenComparingInt(IntCursor::runId));
        List<IntReader> readers = new ArrayList<>(runs.size());
        IOException failure = null;

        try (IntWriter writer = new IntWriter(output)) {
            for (int i = 0; i < runs.size(); i++) {
                IntReader reader = new IntReader(runs.get(i));
                readers.add(reader);
                OptionalInt value = reader.next();
                if (value.isPresent()) {
                    queue.add(new IntCursor(i, value.getAsInt()));
                }
            }

            boolean haveLast = false;
            int last = 0;
            while (!queue.isEmpty()) {
                IntCursor cursor = queue.poll();
                if (!haveLast || cursor.value() != last) {
                    writer.write(cursor.value());
                    last = cursor.value();
                    haveLast = true;
                }

                OptionalInt next = readers.get(cursor.runId()).next();
                if (next.isPresent()) {
                    queue.add(new IntCursor(cursor.runId(), next.getAsInt()));
                }
            }
        } catch (IOException ex) {
            failure = ex;
            throw ex;
        } finally {
            for (IntReader reader : readers) {
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

    private long mergeUnique(List<Path> runs, Path verticesOutput, Path mappingOutput) throws IOException {
        if (verticesOutput.getParent() != null) {
            Files.createDirectories(verticesOutput.getParent());
        }
        if (mappingOutput.getParent() != null) {
            Files.createDirectories(mappingOutput.getParent());
        }

        PriorityQueue<IntCursor> queue = new PriorityQueue<>(Comparator
                .comparingInt(IntCursor::value)
                .thenComparingInt(IntCursor::runId));
        List<IntReader> readers = new ArrayList<>(runs.size());
        IOException failure = null;

        try (IntWriter verticesWriter = new IntWriter(verticesOutput);
             MappingWriter mappingWriter = new MappingWriter(mappingOutput)) {
            for (int i = 0; i < runs.size(); i++) {
                IntReader reader = new IntReader(runs.get(i));
                readers.add(reader);
                OptionalInt value = reader.next();
                if (value.isPresent()) {
                    queue.add(new IntCursor(i, value.getAsInt()));
                }
            }

            boolean haveLast = false;
            int last = 0;
            long denseId = 0;
            while (!queue.isEmpty()) {
                IntCursor cursor = queue.poll();
                if (!haveLast || cursor.value() != last) {
                    if (denseId > Integer.MAX_VALUE) {
                        throw new IOException("too many unique vertices: dense id exceeds int32 range");
                    }
                    verticesWriter.write(cursor.value());
                    mappingWriter.write(cursor.value(), Math.toIntExact(denseId));
                    denseId++;
                    last = cursor.value();
                    haveLast = true;
                }

                OptionalInt next = readers.get(cursor.runId()).next();
                if (next.isPresent()) {
                    queue.add(new IntCursor(cursor.runId(), next.getAsInt()));
                }
            }
            return denseId;
        } catch (IOException ex) {
            failure = ex;
            throw ex;
        } finally {
            for (IntReader reader : readers) {
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

    public record SortUniqueResult(long uniqueCount) {
    }

    private record IntCursor(int runId, int value) {
    }

    private static final class IntReader implements Closeable {
        private final DataInputStream input;

        private IntReader(Path path) throws IOException {
            validateRecordAlignment(path, Integer.BYTES);
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        private OptionalInt next() throws IOException {
            try {
                return OptionalInt.of(input.readInt());
            } catch (EOFException ex) {
                return OptionalInt.empty();
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static void validateRecordAlignment(Path path, int recordBytes) throws IOException {
        long size = Files.size(path);
        if (size % recordBytes != 0) {
            throw new IOException("corrupted file: %s, size=%d, recordBytes=%d"
                    .formatted(path, size, recordBytes));
        }
    }

    private static final class IntWriter implements Closeable {
        private final DataOutputStream output;

        private IntWriter(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        private void write(int value) throws IOException {
            output.writeInt(value);
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

    private static final class MappingWriter implements Closeable {
        private final DataOutputStream output;

        private MappingWriter(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        private void write(int originalId, int denseId) throws IOException {
            output.writeInt(originalId);
            output.writeInt(denseId);
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }
}
