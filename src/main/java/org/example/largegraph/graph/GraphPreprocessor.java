package org.example.largegraph.graph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.io.BinaryEdgeReader;
import org.example.largegraph.io.BinaryEdgeReader.DenseEdge;
import org.example.largegraph.io.CsvEdgeReader;
import org.example.largegraph.io.CsvEdgeReader.Edge;
import org.example.largegraph.io.SourcePartitionWriterManager;
import org.example.largegraph.storage.DiskDoubleArray;
import org.example.largegraph.storage.DiskIntArray;
import org.example.largegraph.util.ProgressLogger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.Properties;

public final class GraphPreprocessor {
    private static final long PROGRESS_INTERVAL_EDGES = 1_000_000L;
    private static final int MAX_RECORD_SORT_CHUNK = 250_000;

    private final AppConfig config;
    private final ProgressLogger logger;

    public GraphPreprocessor(AppConfig config, ProgressLogger logger) {
        this.config = config;
        this.logger = logger;
    }

    public PreprocessingResult preprocess() throws IOException {
        validateInput();
        prepareWorkDir();

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

        rewriteEdgesToDenseSourcePartitions(partitionCount);
        buildOutDegreeBySourcePartition(vertexCount, partitionCount);
        writeSourcePartitionMetadata(partitionCount);
        writeMetadata(vertexCount, stats.edgeCount(), partitionCount);

        logger.info("Preprocessing finished: vertices=%d edges=%d sourcePartitions=%d"
                .formatted(vertexCount, stats.edgeCount(), partitionCount));
        return new PreprocessingResult(
                vertexCount,
                stats.edgeCount(),
                partitionCount,
                partitionCount,
                config.chunkSize(),
                outDegreePath(),
                rankCurrentPath(),
                rankNextPath(),
                edgesDir(),
                verticesPath(),
                mappingPath()
        );
    }

    private FirstPassStats runFirstPass() throws IOException {
        long edgeCount = 0;

        try (CsvEdgeReader reader = new CsvEdgeReader(config.input());
             RawEdgeWriter rawEdges = new RawEdgeWriter(rawEdgesPath());
             IntValueWriter vertexIds = new IntValueWriter(vertexIdsUnsortedPath())) {
            Optional<Edge> edge;
            while ((edge = reader.next()).isPresent()) {
                int from = edge.get().from();
                int to = edge.get().to();
                validateVertexId(from);
                validateVertexId(to);
                rawEdges.write(edgeCount, from, to);
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
        ExternalIntSorter sorter = new ExternalIntSorter(config.chunkSize());
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

    private void rewriteEdgesToDenseSourcePartitions(int partitionCount) throws IOException {
        logger.info("Building endpoint references");
        writeEndpointReferences();

        logger.info("Sorting endpoint references by original id");
        new ExternalRecordSorter<>(
                EndpointRefReader::new,
                EndpointRefWriter::new,
                Comparator.comparingInt(EndpointRef::originalId)
                        .thenComparingLong(EndpointRef::edgeId)
                        .thenComparingInt(record -> record.side()),
                recordSortChunkSize()
        ).sort(
                endpointRefsPath(),
                endpointRefsSortedPath(),
                config.workDir().resolve("sort").resolve("endpoint-refs"),
                "endpoint-ref-run"
        );

        logger.info("Joining endpoints with dense mapping");
        mergeJoinEndpointAssignments();

        logger.info("Sorting endpoint assignments by edge id");
        new ExternalRecordSorter<>(
                EndpointAssignmentReader::new,
                EndpointAssignmentWriter::new,
                Comparator.comparingLong(EndpointAssignment::edgeId)
                        .thenComparingInt(record -> record.side()),
                recordSortChunkSize()
        ).sort(
                endpointAssignmentsPath(),
                endpointAssignmentsSortedPath(),
                config.workDir().resolve("sort").resolve("endpoint-assignments"),
                "endpoint-assignment-run"
        );

        logger.info("Writing dense source partitions");
        writeDenseSourcePartitions(partitionCount);
    }

    private void writeEndpointReferences() throws IOException {
        long edgeCount = 0;
        try (RawEdgeReader reader = new RawEdgeReader(rawEdgesPath());
             EndpointRefWriter writer = new EndpointRefWriter(endpointRefsPath())) {
            Optional<RawEdge> edge;
            while ((edge = reader.next()).isPresent()) {
                writer.write(new EndpointRef(edge.get().fromOriginal(), edge.get().edgeId(), (byte) 0));
                writer.write(new EndpointRef(edge.get().toOriginal(), edge.get().edgeId(), (byte) 1));
                edgeCount++;
                logProgress("endpoint-refs", edgeCount);
            }
        }
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

    private void writeDenseSourcePartitions(int partitionCount) throws IOException {
        int maxOpenWriters = Math.min(64, Math.max(4, config.threads() * 4));
        long edgeCount = 0;

        try (EndpointAssignmentReader reader = new EndpointAssignmentReader(endpointAssignmentsSortedPath());
             SourcePartitionWriterManager writerManager =
                     new SourcePartitionWriterManager(config.workDir(), partitionCount, maxOpenWriters)) {
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
                int sourcePartition = from / config.chunkSize();
                writerManager.write(sourcePartition, from, to);
                edgeCount++;
                logProgress("dense-rewrite", edgeCount);
            }
        }
    }

    private void buildOutDegreeBySourcePartition(long vertexCount, int partitionCount) throws IOException {
        try (DiskIntArray outDegree = new DiskIntArray(outDegreePath(), vertexCount, config.chunkSize())) {
            for (int sourcePartition = 0; sourcePartition < partitionCount; sourcePartition++) {
                long sourceStart = (long) sourcePartition * config.chunkSize();
                int sourceLength = (int) Math.min(config.chunkSize(), vertexCount - sourceStart);
                int[] outDegreeChunk = new int[sourceLength];
                Path sourcePartitionPath = edgesDir().resolve("src-part-%05d.bin".formatted(sourcePartition));
                if (!Files.exists(sourcePartitionPath)) {
                    outDegree.writeIntChunk(sourceStart, outDegreeChunk, sourceLength);
                    continue;
                }

                try (BinaryEdgeReader edgeReader = new BinaryEdgeReader(sourcePartitionPath)) {
                    Optional<DenseEdge> edge;
                    while ((edge = edgeReader.next()).isPresent()) {
                        int localFrom = Math.toIntExact(edge.get().denseFrom() - sourceStart);
                        outDegreeChunk[localFrom]++;
                    }
                }
                outDegree.writeIntChunk(sourceStart, outDegreeChunk, sourceLength);
            }
            outDegree.flush();
        }
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
        if (config.idMode() != AppConfig.IdMode.CONTIGUOUS) {
            throw new IllegalArgumentException("only --id-mode contiguous is supported");
        }
        if (!Files.exists(config.input())) {
            throw new IllegalArgumentException("input file does not exist: " + config.input());
        }
        if (!Files.isRegularFile(config.input())) {
            throw new IllegalArgumentException("input path is not a regular file: " + config.input());
        }
    }

    private int recordSortChunkSize() {
        return Math.min(config.chunkSize(), MAX_RECORD_SORT_CHUNK);
    }

    private void validateVertexId(int vertexId) {
        if (vertexId < 0) {
            throw new IllegalArgumentException("negative vertex id: " + vertexId);
        }
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
        Files.deleteIfExists(rawEdgesPath());
        Files.deleteIfExists(vertexIdsUnsortedPath());
        Files.deleteIfExists(verticesPath());
        Files.deleteIfExists(mappingPath());
        Files.deleteIfExists(endpointRefsPath());
        Files.deleteIfExists(endpointRefsSortedPath());
        Files.deleteIfExists(endpointAssignmentsPath());
        Files.deleteIfExists(endpointAssignmentsSortedPath());
        Files.createDirectories(edgesDir());
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
        properties.setProperty("mapping", mappingPath().toString());
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

    private Path rawEdgesPath() {
        return config.workDir().resolve("raw_edges.bin");
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
        if (!Files.exists(path)) {
            return;
        }
        try (var paths = Files.walk(path)) {
            for (Path child : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(child);
            }
        }
    }

    private record FirstPassStats(long edgeCount) {
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
            Path mappingPath
    ) {
    }

    private record RawEdge(long edgeId, int fromOriginal, int toOriginal) {
    }

    private record MappingRecord(int originalId, int denseId) {
    }

    private record EndpointRef(int originalId, long edgeId, byte side) {
    }

    private record EndpointAssignment(long edgeId, byte side, int denseId) {
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

    private static final class RawEdgeWriter implements Closeable {
        private final DataOutputStream output;

        private RawEdgeWriter(Path path) throws IOException {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
        }

        private void write(long edgeId, int fromOriginal, int toOriginal) throws IOException {
            output.writeLong(edgeId);
            output.writeInt(fromOriginal);
            output.writeInt(toOriginal);
        }

        @Override
        public void close() throws IOException {
            output.close();
        }
    }

    private static final class RawEdgeReader implements Closeable {
        private final DataInputStream input;

        private RawEdgeReader(Path path) throws IOException {
            this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
        }

        private Optional<RawEdge> next() throws IOException {
            try {
                return Optional.of(new RawEdge(input.readLong(), input.readInt(), input.readInt()));
            } catch (EOFException ex) {
                return Optional.empty();
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
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
}
