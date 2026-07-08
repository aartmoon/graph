package org.example.largegraph.graph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.io.BinaryEdgeReader;
import org.example.largegraph.io.CsvEdgeReader;
import org.example.largegraph.io.SourcePartitionWriterManager;
import org.example.largegraph.storage.DiskDoubleArray;
import org.example.largegraph.storage.DiskIntArray;
import org.example.largegraph.util.FileUtils;
import org.example.largegraph.util.ProgressLogger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class GraphPreprocessor {
    private static final long PROGRESS_INTERVAL_EDGES = 1_000_000L;
    private static final int MAX_INT_SORT_CHUNK = 500_000;
    private static final int MAX_RECORD_SORT_CHUNK = 250_000;
    private static final int MAX_TOTAL_SOURCE_PARTITION_WRITERS = 256;

    private final AppConfig config;
    private final ProgressLogger logger;

    public GraphPreprocessor(AppConfig config, ProgressLogger logger) {
        this.config = config;
        this.logger = logger;
    }

    public PreprocessingResult preprocess() throws IOException {
        validateInput();
        prepareWorkDir();
        checkFreeDiskSpace();

        boolean success = false;
        try {
            FirstPassStats stats = runFirstPass();
            ExternalIntSorter.SortUniqueResult vertices = sortVertices();
            long vertexCount = vertices.uniqueCount();
            int partitionCount = partitionCount(vertexCount);

            Path vertexDir = config.workDir().resolve("vertex");
            Files.createDirectories(vertexDir);

            try (DiskIntArray outDegree = new DiskIntArray(outDegreePath(), vertexCount, config.chunkSize());
                 DiskDoubleArray rankCurrent = new DiskDoubleArray(rankCurrentPath(), vertexCount, config.chunkSize());
                 DiskDoubleArray rankNext = new DiskDoubleArray(rankNextPath(), vertexCount, config.chunkSize())) {
                outDegree.fill(0);
                rankCurrent.fill(1.0 / vertexCount);
                rankNext.fill(0.0);
            }

            long edgeCount = rewriteEdgesToDenseSourcePartitions(partitionCount);
            buildOutDegreeBySourcePartition(vertexCount, partitionCount);
            writeSourcePartitionMetadata(partitionCount);
            writeMetadata(vertexCount, edgeCount, partitionCount);
            cleanupPreprocessingIntermediates();

            logger.info("Preprocessing finished: vertices=%d inputEdges=%d storedEdges=%d sourcePartitions=%d"
                    .formatted(vertexCount, stats.edgeCount(), edgeCount, partitionCount));
            success = true;
            return new PreprocessingResult(
                    vertexCount,
                    edgeCount,
                    partitionCount,
                    partitionCount,
                    config.chunkSize(),
                    outDegreePath(),
                    rankCurrentPath(),
                    rankNextPath(),
                    edgesDir(),
                    verticesPath()
            );
        } finally {
            if (!success) {
                cleanupPreprocessingIntermediatesSafely();
            }
        }
    }

    private FirstPassStats runFirstPass() throws IOException {
        long edgeCount = 0;

        try (CsvEdgeReader reader = new CsvEdgeReader(config.input());
             EndpointRefWriter endpointRefs = new EndpointRefWriter(endpointRefsPath());
             IntValueWriter vertexIds = new IntValueWriter(vertexIdsUnsortedPath())) {
            while (reader.next()) {
                int from = reader.from();
                int to = reader.to();
                endpointRefs.write(new EndpointRef(from, edgeCount, (byte) 0));
                endpointRefs.write(new EndpointRef(to, edgeCount, (byte) 1));
                vertexIds.write(from);
                vertexIds.write(to);
                edgeCount++;
                logProgress("pass-1", edgeCount);
            }
        }

        if (edgeCount == 0) {
            throw new IllegalArgumentException("empty graph: input contains no edges");
        }
        logger.info("Pass 1 finished: edges=%d".formatted(edgeCount));
        return new FirstPassStats(edgeCount);
    }

    private ExternalIntSorter.SortUniqueResult sortVertices() throws IOException {
        logger.info("Sorting and deduplicating observed vertex ids");
        ExternalIntSorter sorter = new ExternalIntSorter(intSortChunkSize());
        ExternalIntSorter.SortUniqueResult result = sorter.sortUniqueToVerticesAndMapping(
                vertexIdsUnsortedPath(),
                verticesPath(),
                mappingPath(),
                config.workDir().resolve("sort").resolve("vertices")
        );
        if (result.uniqueCount() == 0) {
            throw new IllegalArgumentException("empty graph: input contains no vertices");
        }
        logger.info("Vertex reindexing finished: observedVertices=%d".formatted(result.uniqueCount()));
        return result;
    }

    private long rewriteEdgesToDenseSourcePartitions(int partitionCount) throws IOException {
        logger.info("Sorting endpoint references by original id");
        new EndpointRefExternalSorter(recordSortChunkSize()).sort(
                endpointRefsPath(),
                endpointRefsSortedPath(),
                config.workDir().resolve("sort").resolve("endpoint-refs"),
                "endpoint-ref-run"
        );

        logger.info("Joining endpoints with dense mapping");
        mergeJoinEndpointAssignments();

        logger.info("Sorting endpoint assignments by edge id");
        new EndpointAssignmentExternalSorter(recordSortChunkSize()).sort(
                endpointAssignmentsPath(),
                endpointAssignmentsSortedPath(),
                config.workDir().resolve("sort").resolve("endpoint-assignments"),
                "endpoint-assignment-run"
        );

        logger.info("Writing dense edge stream");
        writeDenseEdges();

        logger.info("Sorting and deduplicating dense edges");
        new DenseEdgeExternalSorter(recordSortChunkSize()).sortUnique(
                denseEdgesPath(),
                denseEdgesSortedPath(),
                config.workDir().resolve("sort").resolve("dense-edges"),
                "dense-edge-run"
        );

        logger.info("Writing dense source partitions");
        return writeDenseSourcePartitions(partitionCount);
    }

    private void mergeJoinEndpointAssignments() throws IOException {
        try (EndpointRefReader endpoints = new EndpointRefReader(endpointRefsSortedPath());
             MappingReader mapping = new MappingReader(mappingPath());
             EndpointAssignmentWriter assignments = new EndpointAssignmentWriter(endpointAssignmentsPath())) {
            Optional<MappingRecord> mappingRecord = mapping.next();
            Optional<EndpointRef> endpointRecord;
            while ((endpointRecord = endpoints.next()).isPresent()) {
                int originalId = endpointRecord.get().originalId();
                while (mappingRecord.isPresent() && mappingRecord.get().originalId() < originalId) {
                    mappingRecord = mapping.next();
                }
                if (mappingRecord.isEmpty() || mappingRecord.get().originalId() != originalId) {
                    throw new IOException("missing dense id for original vertex id: " + originalId);
                }
                assignments.write(new EndpointAssignment(
                        endpointRecord.get().edgeId(),
                        endpointRecord.get().side(),
                        mappingRecord.get().denseId()
                ));
            }
        }
    }

    private void writeDenseEdges() throws IOException {
        long edgeCount = 0;

        try (EndpointAssignmentReader reader = new EndpointAssignmentReader(endpointAssignmentsSortedPath());
             DenseEdgeWriter writer = new DenseEdgeWriter(denseEdgesPath())) {
            Optional<EndpointAssignment> fromRecord;
            while ((fromRecord = reader.next()).isPresent()) {
                Optional<EndpointAssignment> toRecord = reader.next();
                if (toRecord.isEmpty()) {
                    throw new IOException("dangling endpoint assignment for edge " + fromRecord.get().edgeId());
                }
                if (fromRecord.get().edgeId() != toRecord.get().edgeId()
                        || fromRecord.get().side() != 0
                        || toRecord.get().side() != 1) {
                    throw new IOException("corrupt endpoint assignments around edge " + fromRecord.get().edgeId());
                }
                int from = fromRecord.get().denseId();
                int to = toRecord.get().denseId();
                writer.write(new DenseEdge(from, to));
                edgeCount++;
                logProgress("dense-rewrite", edgeCount);
            }
        }
    }

    private long writeDenseSourcePartitions(int partitionCount) throws IOException {
        int maxOpenWriters = Math.min(partitionCount, MAX_TOTAL_SOURCE_PARTITION_WRITERS);
        long edgeCount = 0;

        try (DenseEdgeReader reader = new DenseEdgeReader(denseEdgesSortedPath());
             SourcePartitionWriterManager writerManager =
                     new SourcePartitionWriterManager(config.workDir(), partitionCount, maxOpenWriters)) {
            Optional<DenseEdge> edge;
            while ((edge = reader.next()).isPresent()) {
                DenseEdge current = edge.get();
                int sourcePartition = current.from() / config.chunkSize();
                writerManager.write(sourcePartition, current.from(), current.to());
                edgeCount++;
                logProgress("dense-dedup-write", edgeCount);
            }
        }
        return edgeCount;
    }

    private void buildOutDegreeBySourcePartition(long vertexCount, int partitionCount) throws IOException {
        int taskCount = Math.min(config.threads(), Math.max(1, partitionCount));
        ExecutorService executor = Executors.newFixedThreadPool(taskCount);
        try {
            List<PartitionRange> ranges = partitionRanges(partitionCount, taskCount);
            List<Callable<Void>> tasks = new ArrayList<>(ranges.size());
            for (PartitionRange range : ranges) {
                tasks.add(() -> {
                    buildOutDegreeForSourcePartitionRange(vertexCount, range);
                    return null;
                });
            }
            invokeAll(executor, tasks);
        } finally {
            shutdown(executor);
        }
    }

    private List<PartitionRange> partitionRanges(int partitionCount, int rangeCount) {
        List<PartitionRange> ranges = new ArrayList<>(rangeCount);
        int partitionsPerRange = Math.ceilDiv(partitionCount, rangeCount);
        for (int range = 0; range < rangeCount; range++) {
            int start = range * partitionsPerRange;
            int end = Math.min(partitionCount, start + partitionsPerRange);
            if (start < end) {
                ranges.add(new PartitionRange(start, end));
            }
        }
        return ranges;
    }

    private void buildOutDegreeForSourcePartitionRange(long vertexCount, PartitionRange range) throws IOException {
        int[] outDegreeChunk = new int[config.chunkSize()];
        ByteBuffer scratch = ByteBuffer.allocate(config.chunkSize() * Integer.BYTES);
        try (DiskIntArray outDegree = new DiskIntArray(outDegreePath(), vertexCount, config.chunkSize(), true, false)) {
            for (int sourcePartition = range.startPartition(); sourcePartition < range.endPartitionExclusive(); sourcePartition++) {
                buildOutDegreeForSourcePartition(outDegree, vertexCount, sourcePartition, outDegreeChunk, scratch);
            }
        }
    }

    private void buildOutDegreeForSourcePartition(
            DiskIntArray outDegree,
            long vertexCount,
            int sourcePartition,
            int[] outDegreeChunk,
            ByteBuffer scratch
    ) throws IOException {
        long sourceStart = (long) sourcePartition * config.chunkSize();
        int sourceLength = (int) Math.min(config.chunkSize(), vertexCount - sourceStart);
        Arrays.fill(outDegreeChunk, 0, sourceLength, 0);
        Path sourcePartitionPath = edgesDir().resolve("src-part-%05d.bin".formatted(sourcePartition));
        if (Files.exists(sourcePartitionPath)) {
            try (BinaryEdgeReader edgeReader = new BinaryEdgeReader(sourcePartitionPath)) {
                while (edgeReader.next()) {
                    int localFrom = Math.toIntExact(edgeReader.denseFrom() - sourceStart);
                    outDegreeChunk[localFrom]++;
                }
            }
        }

        outDegree.writeIntChunk(sourceStart, outDegreeChunk, sourceLength, scratch);
    }

    private void writeSourcePartitionMetadata(int partitionCount) throws IOException {
        Path metadataPath = edgesDir().resolve("source-partitions.tsv");
        try (var writer = Files.newBufferedWriter(metadataPath)) {
            writer.write("partition\tedges\tbytes");
            writer.newLine();
            for (int partition = 0; partition < partitionCount; partition++) {
                Path path = edgesDir().resolve("src-part-%05d.bin".formatted(partition));
                long bytes = Files.exists(path) ? Files.size(path) : 0L;
                writer.write(Integer.toString(partition));
                writer.write('\t');
                writer.write(Long.toString(bytes / (Integer.BYTES * 2L)));
                writer.write('\t');
                writer.write(Long.toString(bytes));
                writer.newLine();
            }
        }
    }

    private void validateInput() {
        if (config.idMode() != AppConfig.IdMode.EXTERNAL_DENSE) {
            throw new IllegalArgumentException("only --id-mode external-dense is supported");
        }
        if (!Files.exists(config.input())) {
            throw new IllegalArgumentException("input file does not exist: " + config.input());
        }
        if (!Files.isRegularFile(config.input())) {
            throw new IllegalArgumentException("input path is not a regular file: " + config.input());
        }
    }

    private void checkFreeDiskSpace() throws IOException {
        FileStore store = Files.getFileStore(config.workDir());
        long free = store.getUsableSpace();
        long inputSize = Files.size(config.input());
        long estimatedRequired = saturatingMultiply(inputSize, 8L);
        if (free < estimatedRequired) {
            throw new IllegalStateException("not enough disk space: free=%d estimatedRequired=%d"
                    .formatted(free, estimatedRequired));
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
        return Math.min(config.chunkSize(), MAX_RECORD_SORT_CHUNK);
    }

    private int intSortChunkSize() {
        return Math.min(config.chunkSize(), MAX_INT_SORT_CHUNK);
    }

    private void prepareWorkDir() throws IOException {
        try {
            Files.createDirectories(config.workDir());
            Files.createDirectories(config.workDir().resolve("vertex"));
        } catch (IOException ex) {
            throw new IOException("cannot create workdir: " + config.workDir(), ex);
        }
        deleteRecursively(edgesDir());
        deleteRecursively(config.workDir().resolve("messages"));
        deleteRecursively(config.workDir().resolve("sort"));
        Files.deleteIfExists(vertexIdsUnsortedPath());
        Files.deleteIfExists(verticesPath());
        Files.deleteIfExists(mappingPath());
        Files.deleteIfExists(endpointRefsPath());
        Files.deleteIfExists(endpointRefsSortedPath());
        Files.deleteIfExists(endpointAssignmentsPath());
        Files.deleteIfExists(endpointAssignmentsSortedPath());
        Files.deleteIfExists(denseEdgesPath());
        Files.deleteIfExists(denseEdgesSortedPath());
        Files.createDirectories(edgesDir());
    }

    private void cleanupPreprocessingIntermediates() throws IOException {
        Files.deleteIfExists(vertexIdsUnsortedPath());
        Files.deleteIfExists(mappingPath());
        Files.deleteIfExists(endpointRefsPath());
        Files.deleteIfExists(endpointRefsSortedPath());
        Files.deleteIfExists(endpointAssignmentsPath());
        Files.deleteIfExists(endpointAssignmentsSortedPath());
        Files.deleteIfExists(denseEdgesPath());
        Files.deleteIfExists(denseEdgesSortedPath());
        deleteRecursively(config.workDir().resolve("sort"));
    }

    private void cleanupPreprocessingIntermediatesSafely() {
        try {
            cleanupPreprocessingIntermediates();
        } catch (IOException cleanupEx) {
            logger.info("WARNING failed to cleanup preprocessing intermediates: " + cleanupEx.getMessage());
        }
    }

    private void writeMetadata(long vertexCount, long edgeCount, int partitionCount) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("vertexCount", Long.toString(vertexCount));
        properties.setProperty("edgeCount", Long.toString(edgeCount));
        properties.setProperty("chunkSize", Integer.toString(config.chunkSize()));
        properties.setProperty("sourcePartitionCount", Integer.toString(partitionCount));
        properties.setProperty("destinationPartitionCount", Integer.toString(partitionCount));
        properties.setProperty("idMode", "external-dense");
        properties.setProperty("edgeFormat", "int denseFrom,int denseTo");
        properties.setProperty("messageFormat", "int denseTo,double contribution");
        properties.setProperty("vertices", verticesPath().toString());
        try (OutputStream output = Files.newOutputStream(config.workDir().resolve("meta.properties"))) {
            properties.store(output, "Large Graph PageRank metadata");
        }
    }

    private int partitionCount(long vertexCount) {
        long partitions = (vertexCount + config.chunkSize() - 1L) / config.chunkSize();
        if (partitions > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("too many partitions: decrease vertex count or increase --chunk-size");
        }
        return Math.toIntExact(partitions);
    }

    private Path vertexIdsUnsortedPath() {
        return config.workDir().resolve("vertex_ids_unsorted.bin");
    }

    private Path verticesPath() {
        return config.workDir().resolve("vertices.bin");
    }

    private Path mappingPath() {
        return config.workDir().resolve("mapping.bin");
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

    private <T> List<T> invokeAll(ExecutorService executor, List<Callable<T>> tasks) throws IOException {
        try {
            List<Future<T>> futures = executor.invokeAll(tasks);
            List<T> results = new ArrayList<>(futures.size());
            for (Future<T> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("preprocessing was interrupted", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("preprocessing task failed", cause);
        }
    }

    private void shutdown(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        FileUtils.deleteRecursively(path);
    }

    private record FirstPassStats(long edgeCount) {
    }

    private record PartitionRange(int startPartition, int endPartitionExclusive) {
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
            Path verticesPath
    ) {
    }

    private record MappingRecord(int originalId, int denseId) {
    }

    private record EndpointRef(int originalId, long edgeId, byte side) {
    }

    private record EndpointAssignment(long edgeId, byte side, int denseId) {
    }

    private record DenseEdge(int from, int to) {
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

    private static final class MappingReader implements Closeable {
        private final DataInputStream input;

        private MappingReader(Path path) throws IOException {
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        private Optional<MappingRecord> next() throws IOException {
            try {
                return Optional.of(new MappingRecord(input.readInt(), input.readInt()));
            } catch (EOFException ex) {
                return Optional.empty();
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class EndpointRefReader implements ExternalRecordSorter.RecordReader<EndpointRef> {
        private final DataInputStream input;

        private EndpointRefReader(Path path) throws IOException {
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        @Override
        public Optional<EndpointRef> next() throws IOException {
            try {
                return Optional.of(new EndpointRef(input.readInt(), input.readLong(), input.readByte()));
            } catch (EOFException ex) {
                return Optional.empty();
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class EndpointRefWriter implements ExternalRecordSorter.RecordWriter<EndpointRef> {
        private final DataOutputStream output;

        private EndpointRefWriter(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        @Override
        public void write(EndpointRef record) throws IOException {
            output.writeInt(record.originalId());
            output.writeLong(record.edgeId());
            output.writeByte(record.side());
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

    private static final class EndpointAssignmentReader implements ExternalRecordSorter.RecordReader<EndpointAssignment> {
        private final DataInputStream input;

        private EndpointAssignmentReader(Path path) throws IOException {
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        @Override
        public Optional<EndpointAssignment> next() throws IOException {
            try {
                return Optional.of(new EndpointAssignment(input.readLong(), input.readByte(), input.readInt()));
            } catch (EOFException ex) {
                return Optional.empty();
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class EndpointAssignmentWriter implements ExternalRecordSorter.RecordWriter<EndpointAssignment> {
        private final DataOutputStream output;

        private EndpointAssignmentWriter(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        @Override
        public void write(EndpointAssignment record) throws IOException {
            output.writeLong(record.edgeId());
            output.writeByte(record.side());
            output.writeInt(record.denseId());
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

    private static final class DenseEdgeReader implements ExternalRecordSorter.RecordReader<DenseEdge> {
        private final DataInputStream input;

        private DenseEdgeReader(Path path) throws IOException {
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        @Override
        public Optional<DenseEdge> next() throws IOException {
            try {
                return Optional.of(new DenseEdge(input.readInt(), input.readInt()));
            } catch (EOFException ex) {
                return Optional.empty();
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }

    private static final class DenseEdgeWriter implements ExternalRecordSorter.RecordWriter<DenseEdge> {
        private final DataOutputStream output;

        private DenseEdgeWriter(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        @Override
        public void write(DenseEdge record) throws IOException {
            output.writeInt(record.from());
            output.writeInt(record.to());
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }
}
