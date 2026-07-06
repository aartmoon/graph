package org.example.largegraph.graph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.io.CsvEdgeReader;
import org.example.largegraph.io.CsvEdgeReader.Edge;
import org.example.largegraph.util.ProgressLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public final class GraphPreprocessor {
    private static final long PROGRESS_INTERVAL_EDGES = 1_000_000L;

    private final AppConfig config;
    private final ProgressLogger logger;

    public GraphPreprocessor(AppConfig config, ProgressLogger logger) {
        this.config = config;
        this.logger = logger;
    }

    public PreprocessingResult preprocess() throws IOException {
        validateInput();
        ensureWorkDir();

        IndexBuildResult indexBuildResult = buildIndex();
        VertexIndexer indexer = indexBuildResult.indexer();
        int[] outDegree = new int[indexer.size()];
        long edgeCount = partitionEdges(indexer, outDegree);

        new OutDegreeStore(config.workDir()).write(outDegree);
        new VertexIdStore(config.workDir()).write(indexer);
        new RankStore(config.workDir()).initializeUniform(indexer.size());

        logger.info("Preprocessing finished: vertices=%d edges=%d".formatted(indexer.size(), edgeCount));
        return new PreprocessingResult(indexer, edgeCount, indexer.size(), config.partitions());
    }

    private void validateInput() {
        if (!Files.exists(config.input())) {
            throw new IllegalArgumentException("input file does not exist: " + config.input());
        }
        if (!Files.isRegularFile(config.input())) {
            throw new IllegalArgumentException("input path is not a regular file: " + config.input());
        }
    }

    private void ensureWorkDir() throws IOException {
        Path workDir = config.workDir();
        try {
            Files.createDirectories(workDir);
        } catch (IOException ex) {
            throw new IOException("cannot create workdir: " + workDir, ex);
        }
        if (!Files.isDirectory(workDir)) {
            throw new IOException("workdir path is not a directory: " + workDir);
        }
        if (!Files.isWritable(workDir)) {
            throw new IOException("workdir is not writable: " + workDir);
        }
    }

    private IndexBuildResult buildIndex() throws IOException {
        VertexIndexer indexer = new VertexIndexer();
        long edgeCount = 0;
        try (CsvEdgeReader reader = new CsvEdgeReader(config.input())) {
            Optional<Edge> edge;
            while ((edge = reader.next()).isPresent()) {
                validateVertexId(edge.get().from());
                validateVertexId(edge.get().to());
                indexer.getOrCreateDenseId(edge.get().from());
                indexer.getOrCreateDenseId(edge.get().to());
                edgeCount++;
                logProgress("index", edgeCount);
            }
        }
        if (edgeCount == 0) {
            throw new IllegalArgumentException("empty graph: input contains no edges");
        }
        logger.info("Index pass finished: vertices=%d edges=%d".formatted(indexer.size(), edgeCount));
        return new IndexBuildResult(indexer, edgeCount);
    }

    private long partitionEdges(VertexIndexer indexer, int[] outDegree) throws IOException {
        long edgeCount = 0;
        try (CsvEdgeReader reader = new CsvEdgeReader(config.input());
             EdgePartitioner partitioner = new EdgePartitioner(config.workDir(), config.partitions())) {
            Optional<Edge> edge;
            while ((edge = reader.next()).isPresent()) {
                int denseFrom = indexer.denseId(edge.get().from());
                int denseTo = indexer.denseId(edge.get().to());
                outDegree[denseFrom]++;
                partitioner.write(denseFrom, denseTo);
                edgeCount++;
                logProgress("partition", edgeCount);
            }
        }
        logger.info("Partition pass finished: edges=%d partitions=%d".formatted(edgeCount, config.partitions()));
        return edgeCount;
    }

    private void validateVertexId(int vertexId) {
        if (config.idMode() == AppConfig.IdMode.CONTIGUOUS && vertexId < 0) {
            throw new IllegalArgumentException("negative vertex id in contiguous mode: " + vertexId);
        }
    }

    private void logProgress(String phase, long edgeCount) {
        if (edgeCount % PROGRESS_INTERVAL_EDGES == 0) {
            logger.info("%s pass processed %,d edges".formatted(phase, edgeCount));
        }
    }

    public record PreprocessingResult(
            VertexIndexer vertexIndexer,
            long edgeCount,
            int vertexCount,
            int partitionCount
    ) {
    }

    private record IndexBuildResult(VertexIndexer indexer, long edgeCount) {
    }
}
