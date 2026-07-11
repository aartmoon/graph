package org.example.largegraph.graph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.io.BinaryEdgeReader;
import org.example.largegraph.io.BinaryEdgeWriter;
import org.example.largegraph.io.CsvEdgeReader;
import org.example.largegraph.storage.DiskDoubleArray;
import org.example.largegraph.storage.DiskIntArray;
import org.example.largegraph.util.FileUtils;
import org.example.largegraph.util.MemoryUtils;
import org.example.largegraph.util.ProgressLogger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public final class GraphPreprocessor {
    private static final long PROGRESS_INTERVAL_EDGES = 1_000_000L;
    private static final int MAX_RECORD_SORT_CHUNK = 250_000;
    private static final int MIN_SORT_CHUNK = 10_000;

    private final AppConfig config;
    private final ProgressLogger logger;

    public GraphPreprocessor(AppConfig config, ProgressLogger logger) {
        this.config = config;
        this.logger = logger;
    }

    public PreprocessingResult preprocess() throws IOException {
        validateInput();
        validatePathLayout();
        checkInitialFreeDiskSpace();

        boolean success = false;
        try {
            FirstPassStats stats = runFirstPass();
            checkPostFirstPassFreeDiskSpace(stats.edgeCount());
            RewriteStats rewriteStats = rewriteEdgesToDenseSourcePartitions();
            if (rewriteStats.reconstructedInputEdges() != stats.edgeCount()) {
                throw new IOException("dense rewrite lost input edges: expected=%d actual=%d"
                        .formatted(stats.edgeCount(), rewriteStats.reconstructedInputEdges()));
            }
            long vertexCount = rewriteStats.vertexCount();
            int partitionCount = partitionCount(vertexCount);
            validatePartitionMetadataBudget(partitionCount);
            checkSourceAndRankFreeDiskSpace(vertexCount, rewriteStats.storedEdges());
            SourceStorageStats sourceStorage = buildSourceStorageAndOutDegree(vertexCount, partitionCount);
            if (sourceStorage.edgeCount() != rewriteStats.storedEdges()) {
                throw new IOException("source storage edge count mismatch: expected=%d actual=%d"
                        .formatted(rewriteStats.storedEdges(), sourceStorage.edgeCount()));
            }
            createAndInitializeRankFiles(vertexCount);

            logger.info("Preprocessing finished: vertices=%d inputEdges=%d storedEdges=%d sourcePartitions=%d"
                    .formatted(vertexCount, stats.edgeCount(), rewriteStats.storedEdges(), partitionCount));
            success = true;
            return new PreprocessingResult(
                    vertexCount,
                    rewriteStats.storedEdges(),
                    partitionCount,
                    partitionCount,
                    config.chunkSize(),
                    outDegreePath(),
                    rankCurrentPath(),
                    rankNextPath(),
                    edgesDir(),
                    verticesPath(),
                    sourceStorage.sourcePartitions()
            );
        } finally {
            if (!success) {
                cleanupFailedRunArtifactsSafely();
            }
        }
    }

    private FirstPassStats runFirstPass() throws IOException {
        long edgeCount = 0;

        try (CsvEdgeReader reader = new CsvEdgeReader(config.input());
             EndpointRefWriter endpointRefs = new EndpointRefWriter(endpointRefsPath())) {
            while (reader.next()) {
                int from = reader.from();
                int to = reader.to();
                endpointRefs.write(from, edgeCount, (byte) 0);
                endpointRefs.write(to, edgeCount, (byte) 1);
                edgeCount++;
                logProgress("pass-1", edgeCount);
            }
        }

        if (edgeCount == 0) {
            throw new IOException("empty graph: input contains no edges");
        }
        logger.info("Pass 1 finished: edges=%d".formatted(edgeCount));
        return new FirstPassStats(edgeCount);
    }

    private RewriteStats rewriteEdgesToDenseSourcePartitions() throws IOException {
        logger.info("Sorting endpoint references by original id");
        ensureSpaceForExternalSort(endpointRefsPath(), "endpoint reference sort");
        new EndpointRefExternalSorter(recordSortChunkSize()).sort(
                endpointRefsPath(),
                endpointRefsSortedPath(),
                config.workDir().resolve("sort").resolve("endpoint-refs"),
                "endpoint-ref-run"
        );
        Files.deleteIfExists(endpointRefsPath());

        logger.info("Assigning dense vertex ids from sorted endpoints");
        long vertexCount = writeVerticesAndEndpointAssignments();
        Files.deleteIfExists(endpointRefsSortedPath());

        logger.info("Sorting endpoint assignments by edge id");
        ensureSpaceForExternalSort(endpointAssignmentsPath(), "endpoint assignment sort");
        new EndpointAssignmentExternalSorter(recordSortChunkSize()).sort(
                endpointAssignmentsPath(),
                endpointAssignmentsSortedPath(),
                config.workDir().resolve("sort").resolve("endpoint-assignments"),
                "endpoint-assignment-run"
        );
        Files.deleteIfExists(endpointAssignmentsPath());

        logger.info("Writing dense edge stream");
        long reconstructedInputEdges = writeDenseEdges();
        Files.deleteIfExists(endpointAssignmentsSortedPath());

        logger.info("Sorting and deduplicating dense edges");
        ensureSpaceForExternalSort(denseEdgesPath(), "dense edge sort");
        new DenseEdgeExternalSorter(recordSortChunkSize()).sortUnique(
                denseEdgesPath(),
                denseEdgesSortedPath(),
                config.workDir().resolve("sort").resolve("dense-edges"),
                "dense-edge-run"
        );
        Files.deleteIfExists(denseEdgesPath());
        deleteRecursively(config.workDir().resolve("sort"));

        long storedEdges = Files.size(denseEdgesSortedPath()) / (Integer.BYTES * 2L);
        logger.info("Dense edge rewrite finished: vertices=%d storedEdges=%d".formatted(vertexCount, storedEdges));
        return new RewriteStats(vertexCount, reconstructedInputEdges, storedEdges);
    }

    private long writeVerticesAndEndpointAssignments() throws IOException {
        long vertexCount = 0;
        boolean havePrevious = false;
        int previousOriginalId = 0;
        int denseId = -1;

        try (EndpointRefReader endpoints = new EndpointRefReader(endpointRefsSortedPath());
             IntValueWriter vertices = new IntValueWriter(verticesPath());
             EndpointAssignmentWriter assignments = new EndpointAssignmentWriter(endpointAssignmentsPath())) {
            while (endpoints.next()) {
                int originalId = endpoints.originalId();
                if (!havePrevious || originalId != previousOriginalId) {
                    if (denseId == Integer.MAX_VALUE - 1) {
                        throw new IOException("vertex count exceeds supported int dense id model");
                    }
                    denseId++;
                    vertices.write(originalId);
                    previousOriginalId = originalId;
                    havePrevious = true;
                    vertexCount++;
                }
                assignments.write(endpoints.edgeId(), endpoints.side(), denseId);
            }
        }
        if (vertexCount == 0) {
            throw new IOException("empty graph: input contains no vertices");
        }
        logger.info("Vertex reindexing finished: observedVertices=%d".formatted(vertexCount));
        return vertexCount;
    }

    private SourceStorageStats buildSourceStorageAndOutDegree(long vertexCount, int partitionCount) throws IOException {
        logger.info("Writing dense source partitions and out-degree");
        List<SourcePartitionInfo> sourcePartitions = new ArrayList<>();
        long edgeCount = 0;
        boolean hasNextEdge;

        try (BinaryEdgeReader reader = new BinaryEdgeReader(denseEdgesSortedPath());
             DiskIntArray outDegree = new DiskIntArray(outDegreePath(), vertexCount, config.chunkSize())) {
            int[] outDegreeChunk = new int[config.chunkSize()];
            ByteBuffer scratch = ByteBuffer.allocate(config.chunkSize() * Integer.BYTES);
            hasNextEdge = reader.next();
            for (int partition = 0; partition < partitionCount; partition++) {
                long sourceStart = (long) partition * config.chunkSize();
                long sourceEnd = Math.min(vertexCount, sourceStart + config.chunkSize());
                int sourceLength = (int) (sourceEnd - sourceStart);
                Arrays.fill(outDegreeChunk, 0, sourceLength, 0);

                PartitionWriteResult result = writeSourcePartition(
                        reader,
                        hasNextEdge,
                        partition,
                        sourceStart,
                        sourceEnd,
                        outDegreeChunk
                );
                hasNextEdge = result.hasNextEdge();
                edgeCount += result.edgeCount();
                outDegree.writeIntChunk(sourceStart, outDegreeChunk, sourceLength, scratch);
                if (result.edgeCount() > 0) {
                    sourcePartitions.add(new SourcePartitionInfo(
                            partition,
                            result.edgeCount() * Integer.BYTES * 2L
                    ));
                }
                logProgress("dense-dedup-write", edgeCount);
            }
            if (hasNextEdge) {
                throw new IOException("dense edge source partition out of range: " + reader.denseFrom());
            }
        }
        Files.deleteIfExists(denseEdgesSortedPath());
        return new SourceStorageStats(edgeCount, List.copyOf(sourcePartitions));
    }

    private PartitionWriteResult writeSourcePartition(
            BinaryEdgeReader reader,
            boolean hasFirstEdge,
            int partition,
            long sourceStart,
            long sourceEnd,
            int[] outDegreeChunk
    ) throws IOException {
        boolean hasEdge = hasFirstEdge;
        long edgeCount = 0;
        BinaryEdgeWriter writer = null;
        IOException failure = null;
        Path path = edgesDir().resolve("src-part-%05d.bin".formatted(partition));
        try {
            while (hasEdge && reader.denseFrom() < sourceEnd) {
                int from = reader.denseFrom();
                int to = reader.denseTo();
                if (from < sourceStart) {
                    throw new IOException("dense edge source partition order is corrupt: from=" + from);
                }
                if (writer == null) {
                    writer = new BinaryEdgeWriter(path);
                }
                writer.write(from, to);
                int localFrom = Math.toIntExact(from - sourceStart);
                if (outDegreeChunk[localFrom] == Integer.MAX_VALUE) {
                    throw new IOException("out-degree exceeds int32 for dense vertex " + from);
                }
                outDegreeChunk[localFrom]++;
                edgeCount++;
                hasEdge = reader.next();
            }
        } catch (IOException ex) {
            failure = ex;
            throw ex;
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException ex) {
                    if (failure != null) {
                        failure.addSuppressed(ex);
                    } else {
                        throw ex;
                    }
                }
            }
        }
        return new PartitionWriteResult(hasEdge, edgeCount);
    }

    private long writeDenseEdges() throws IOException {
        long edgeCount = 0;

        try (EndpointAssignmentReader reader = new EndpointAssignmentReader(endpointAssignmentsSortedPath());
             BinaryEdgeWriter writer = new BinaryEdgeWriter(denseEdgesPath())) {
            long expectedEdgeId = 0;
            while (reader.next()) {
                long fromEdgeId = reader.edgeId();
                byte fromSide = reader.side();
                int from = reader.denseId();
                if (!reader.next()) {
                    throw new IOException("dangling endpoint assignment for edge " + fromEdgeId);
                }
                if (fromEdgeId != expectedEdgeId
                        || reader.edgeId() != expectedEdgeId
                        || fromSide != 0
                        || reader.side() != 1) {
                    throw new IOException("corrupt endpoint assignments around expected edge " + expectedEdgeId);
                }
                writer.write(from, reader.denseId());
                edgeCount++;
                expectedEdgeId++;
                logProgress("dense-rewrite", edgeCount);
            }
        }
        return edgeCount;
    }

    private void createAndInitializeRankFiles(long vertexCount) throws IOException {
        Files.deleteIfExists(rankNextPath());
        try (DiskDoubleArray rankCurrent = new DiskDoubleArray(rankCurrentPath(), vertexCount, config.chunkSize())) {
            rankCurrent.fill(1.0 / vertexCount);
        }
    }

    private void validateInput() {
        if (!Files.exists(config.input())) {
            throw new IllegalArgumentException("input file does not exist: " + config.input());
        }
        if (!Files.isRegularFile(config.input())) {
            throw new IllegalArgumentException("input path is not a regular file: " + config.input());
        }
    }

    private void validatePathLayout() throws IOException {
        Path input = config.input().toRealPath();
        Files.createDirectories(config.workDir());
        Path work = config.workDir().toRealPath();
        Path output = resolvePossiblyMissingPath(config.output());
        if (input.equals(output)) {
            throw new IllegalArgumentException("input and output must be different files");
        }
        if (input.startsWith(work)) {
            throw new IllegalArgumentException("input must not be located inside workdir");
        }
        if (output.startsWith(work)) {
            throw new IllegalArgumentException("output must not be located inside workdir");
        }
    }

    private Path resolvePossiblyMissingPath(Path path) throws IOException {
        if (Files.exists(path)) {
            return path.toRealPath();
        }
        Path absolute = path.toAbsolutePath().normalize();
        Path fileName = absolute.getFileName();
        Path parent = absolute.getParent();
        if (parent == null) {
            return absolute;
        }
        Path existingParent = parent;
        while (existingParent != null && !Files.exists(existingParent)) {
            existingParent = existingParent.getParent();
        }
        if (existingParent == null) {
            return absolute;
        }
        Path realParent = existingParent.toRealPath();
        Path relative = existingParent.relativize(parent);
        return realParent.resolve(relative).resolve(fileName).normalize();
    }

    private void checkInitialFreeDiskSpace() throws IOException {
        Path probe = Files.exists(config.workDir())
                ? config.workDir()
                : config.workDir().toAbsolutePath().normalize().getParent();
        FileStore store = Files.getFileStore(probe);
        long free = store.getUsableSpace();
        long inputSize = Files.size(config.input());
        long reserveBytes = 64L * 1024L * 1024L;
        requireFreeSpace(free, saturatingAdd(saturatingMultiply(inputSize, 7L), reserveBytes), "initial preprocessing");
    }

    private void checkPostFirstPassFreeDiskSpace(long inputEdges) throws IOException {
        FileStore store = Files.getFileStore(config.workDir());
        long free = store.getUsableSpace();
        long endpointSortBytes = saturatingMultiply(inputEdges, 26L * 2L);
        long assignmentSortBytes = saturatingMultiply(inputEdges, 26L * 2L);
        long denseSortBytes = saturatingMultiply(inputEdges, 16L);
        long safetyReserve = 64L * 1024L * 1024L;
        long required = saturatingAdd(Math.max(endpointSortBytes, Math.max(assignmentSortBytes, denseSortBytes)), safetyReserve);
        requireFreeSpace(free, required, "post first-pass preprocessing");
    }

    private void ensureSpaceForExternalSort(Path input, String phase) throws IOException {
        FileStore store = Files.getFileStore(config.workDir());
        long free = store.getUsableSpace();
        long inputSize = Files.size(input);
        long reserveBytes = 64L * 1024L * 1024L;
        long required = saturatingAdd(saturatingMultiply(inputSize, 2L), reserveBytes);
        requireFreeSpace(free, required, phase);
    }

    private void checkSourceAndRankFreeDiskSpace(long vertexCount, long storedEdges) throws IOException {
        FileStore store = Files.getFileStore(config.workDir());
        long free = store.getUsableSpace();
        long sourcePhaseBytes = saturatingAdd(
                saturatingMultiply(storedEdges, Integer.BYTES * 2L),
                saturatingMultiply(vertexCount, Integer.BYTES)
        );
        long rankPhaseBytes = saturatingMultiply(vertexCount, Double.BYTES);
        long sourceFileBlockBytes = saturatingMultiply(Math.min(storedEdges, partitionCount(vertexCount)), 4_096L);
        long reserveBytes = 64L * 1024L * 1024L;
        long required = saturatingAdd(saturatingAdd(sourcePhaseBytes, rankPhaseBytes), sourceFileBlockBytes);
        requireFreeSpace(free, saturatingAdd(required, reserveBytes), "source storage and rank initialization");
    }

    private void requireFreeSpace(long free, long estimatedRequired, String phase) {
        if (free < estimatedRequired) {
            throw new IllegalStateException("not enough disk space for %s: free=%d estimatedRequired=%d"
                    .formatted(phase, free, estimatedRequired));
        }
    }

    private long saturatingAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

    private long saturatingMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

    private int recordSortChunkSize() {
        long budget = Math.max(1L, MemoryUtils.maxHeapBytes() / 16L);
        long records = budget / 32L;
        return checkedSortChunk(records, MAX_RECORD_SORT_CHUNK, "record external sort");
    }

    private int checkedSortChunk(long calculatedRecords, int maxRecords, String phase) {
        long records = Math.min(maxRecords, calculatedRecords);
        if (records < MIN_SORT_CHUNK) {
            throw new IllegalArgumentException("not enough heap for %s: requiredRecords=%d calculatedRecords=%d"
                    .formatted(phase, MIN_SORT_CHUNK, calculatedRecords));
        }
        return Math.toIntExact(Math.max(MIN_SORT_CHUNK, records));
    }

    private void cleanupPreprocessingIntermediates() throws IOException {
        Files.deleteIfExists(endpointRefsPath());
        Files.deleteIfExists(endpointRefsSortedPath());
        Files.deleteIfExists(endpointAssignmentsPath());
        Files.deleteIfExists(endpointAssignmentsSortedPath());
        Files.deleteIfExists(denseEdgesPath());
        Files.deleteIfExists(denseEdgesSortedPath());
        deleteRecursively(config.workDir().resolve("sort"));
    }

    private void cleanupFailedRunArtifactsSafely() {
        try {
            cleanupPreprocessingIntermediates();
            deleteRecursively(edgesDir());
            deleteRecursively(config.workDir().resolve("vertex"));
            deleteRecursively(config.workDir().resolve("messages"));
            Files.deleteIfExists(verticesPath());
        } catch (IOException cleanupEx) {
            logger.info("WARNING failed to cleanup failed run artifacts: " + cleanupEx.getMessage());
        }
    }

    private int partitionCount(long vertexCount) {
        long partitions = (vertexCount + config.chunkSize() - 1L) / config.chunkSize();
        if (partitions > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("too many partitions: decrease vertex count or increase --chunk-size");
        }
        return Math.toIntExact(partitions);
    }

    private void validatePartitionMetadataBudget(int partitionCount) throws IOException {
        long estimatedMetadataBytes = saturatingMultiply(partitionCount, 48L);
        long metadataBudget = Math.max(1L, MemoryUtils.maxHeapBytes() / 10L);
        if (estimatedMetadataBytes > metadataBudget) {
            throw new IOException("too many source partitions for heap; increase --chunk-size: partitions=%d estimatedMetadata=%s budget=%s"
                    .formatted(
                            partitionCount,
                            MemoryUtils.humanReadableBytes(estimatedMetadataBytes),
                            MemoryUtils.humanReadableBytes(metadataBudget)
                    ));
        }
    }

    private Path verticesPath() {
        return config.workDir().resolve("vertices.bin");
    }

    private Path endpointRefsPath() {
        return config.workDir().resolve("endpoint_refs.bin");
    }

    private Path endpointRefsSortedPath() {
        return config.workDir().resolve("endpoint_refs.sorted.bin");
    }

    private Path endpointAssignmentsPath() {
        return config.workDir().resolve("endpoint_assignments.bin");
    }

    private Path endpointAssignmentsSortedPath() {
        return config.workDir().resolve("endpoint_assignments.sorted.bin");
    }

    private Path denseEdgesPath() {
        return config.workDir().resolve("dense_edges.bin");
    }

    private Path denseEdgesSortedPath() {
        return config.workDir().resolve("dense_edges.sorted.bin");
    }

    private Path outDegreePath() {
        return config.workDir().resolve("vertex").resolve("out_degree.bin");
    }

    private Path rankCurrentPath() {
        return config.workDir().resolve("vertex").resolve("rank_current.bin");
    }

    private Path rankNextPath() {
        return config.workDir().resolve("vertex").resolve("rank_next.bin");
    }

    private Path edgesDir() {
        return config.workDir().resolve("edges_by_source");
    }

    private void logProgress(String phase, long edgeCount) {
        if (edgeCount % PROGRESS_INTERVAL_EDGES == 0) {
            logger.info("%s processed %,d edges".formatted(phase, edgeCount));
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        FileUtils.deleteRecursively(path);
    }

    private record FirstPassStats(long edgeCount) {
    }

    private record RewriteStats(long vertexCount, long reconstructedInputEdges, long storedEdges) {
    }

    private record SourceStorageStats(long edgeCount, List<SourcePartitionInfo> sourcePartitions) {
    }

    private record PartitionWriteResult(boolean hasNextEdge, long edgeCount) {
    }

    public record PreprocessingResult(
            long vertexCount,
            long edgeCount,
            int sourcePartitionCount,
            int destinationPartitionCount,
            int chunkSize,
            Path outDegreePath,
            Path rankCurrentPath,
            Path rankNextPath,
            Path edgesDir,
            Path verticesPath,
            List<SourcePartitionInfo> sourcePartitions
    ) {
    }

    public record SourcePartitionInfo(
            int partition,
            long bytes
    ) {
    }

    private static final class IntValueWriter implements Closeable {
        private final DataOutputStream output;

        private IntValueWriter(Path path) throws IOException {
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

    private static final class EndpointRefReader implements Closeable {
        private final DataInputStream input;
        private int originalId;
        private long edgeId;
        private byte side;

        private EndpointRefReader(Path path) throws IOException {
            validateRecordAlignment(path, Integer.BYTES + Long.BYTES + Byte.BYTES);
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        public boolean next() throws IOException {
            try {
                originalId = input.readInt();
                edgeId = input.readLong();
                side = input.readByte();
                return true;
            } catch (EOFException ex) {
                return false;
            }
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

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class EndpointRefWriter implements Closeable {
        private final DataOutputStream output;

        private EndpointRefWriter(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        public void write(int originalId, long edgeId, byte side) throws IOException {
            output.writeInt(originalId);
            output.writeLong(edgeId);
            output.writeByte(side);
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

    private static final class EndpointAssignmentReader implements Closeable {
        private final DataInputStream input;
        private long edgeId;
        private byte side;
        private int denseId;

        private EndpointAssignmentReader(Path path) throws IOException {
            validateRecordAlignment(path, Long.BYTES + Byte.BYTES + Integer.BYTES);
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        public boolean next() throws IOException {
            try {
                edgeId = input.readLong();
                side = input.readByte();
                denseId = input.readInt();
                return true;
            } catch (EOFException ex) {
                return false;
            }
        }

        private long edgeId() {
            return edgeId;
        }

        private byte side() {
            return side;
        }

        private int denseId() {
            return denseId;
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class EndpointAssignmentWriter implements Closeable {
        private final DataOutputStream output;

        private EndpointAssignmentWriter(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        public void write(long edgeId, byte side, int denseId) throws IOException {
            output.writeLong(edgeId);
            output.writeByte(side);
            output.writeInt(denseId);
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
