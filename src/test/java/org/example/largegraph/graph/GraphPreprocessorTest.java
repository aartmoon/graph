package org.example.largegraph.graph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.storage.DiskIntArray;
import org.example.largegraph.util.ProgressLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GraphPreprocessorTest {
    @TempDir
    Path tempDir;

    @Test
    void createsExternalMemoryPreprocessingFiles() throws IOException {
        Path input = tempDir.resolve("edges.csv");
        Path output = tempDir.resolve("output").resolve("pagerank.csv");
        Path workDir = tempDir.resolve("work");
        Files.writeString(input, """
                from,to
                10,20
                10,30
                20,30
                30,10
                """);

        AppConfig config = config(input, output, workDir, 8, 4);
        GraphPreprocessor.PreprocessingResult result = new GraphPreprocessor(config, new ProgressLogger()).preprocess();

        assertEquals(3, result.vertexCount());
        assertEquals(4, result.edgeCount());
        assertEquals(1, result.sourcePartitionCount());
        assertTrue(Files.exists(workDir.resolve("meta.properties")));
        assertTrue(Files.exists(workDir.resolve("vertices.bin")));
        assertTrue(Files.exists(workDir.resolve("vertex").resolve("out_degree.bin")));
        assertTrue(Files.exists(workDir.resolve("vertex").resolve("rank_current.bin")));
        assertTrue(Files.exists(workDir.resolve("vertex").resolve("rank_next.bin")));
        assertTrue(Files.exists(workDir.resolve("edges_by_source").resolve("src-part-00000.bin")));
        assertFalse(Files.exists(workDir.resolve("raw_edges.bin")));
        assertFalse(Files.exists(workDir.resolve("vertex_ids_unsorted.bin")));
        assertFalse(Files.exists(workDir.resolve("mapping.bin")));
        assertFalse(Files.exists(workDir.resolve("endpoint_refs.bin")));
        assertFalse(Files.exists(workDir.resolve("endpoint_refs.sorted.bin")));
        assertFalse(Files.exists(workDir.resolve("endpoint_assignments.bin")));
        assertFalse(Files.exists(workDir.resolve("endpoint_assignments.sorted.bin")));
        assertFalse(Files.exists(workDir.resolve("dense_edges.bin")));
        assertFalse(Files.exists(workDir.resolve("dense_edges.sorted.bin")));
        assertFalse(Files.exists(workDir.resolve("sort")));

        try (DiskIntArray outDegree = new DiskIntArray(result.outDegreePath(), result.vertexCount(), result.chunkSize())) {
            assertEquals(2, outDegree.getInt(0));
            assertEquals(1, outDegree.getInt(1));
            assertEquals(1, outDegree.getInt(2));
        }
        try (DiskIntArray vertices = new DiskIntArray(result.verticesPath(), result.vertexCount(), result.chunkSize())) {
            assertEquals(10, vertices.getInt(0));
            assertEquals(20, vertices.getInt(1));
            assertEquals(30, vertices.getInt(2));
        }
    }

    @Test
    void duplicateEdgesAreDeduplicatedForUnweightedGraph() throws IOException {
        Path input = tempDir.resolve("duplicates.csv");
        Path output = tempDir.resolve("output").resolve("pagerank.csv");
        Path workDir = tempDir.resolve("duplicates-work");
        Files.writeString(input, """
                from,to
                1,2
                1,2
                1,3
                """);

        AppConfig config = config(input, output, workDir, 4, 2);
        GraphPreprocessor.PreprocessingResult result = new GraphPreprocessor(config, new ProgressLogger()).preprocess();

        assertEquals(2, result.edgeCount());
        try (DiskIntArray outDegree = new DiskIntArray(result.outDegreePath(), result.vertexCount(), result.chunkSize())) {
            assertEquals(2, outDegree.getInt(0));
            assertEquals(0, outDegree.getInt(1));
            assertEquals(0, outDegree.getInt(2));
        }
    }

    @Test
    void sparseOriginalIdsCreateOnlyObservedDenseVertices() throws IOException {
        Path input = tempDir.resolve("sparse.csv");
        Path output = tempDir.resolve("output").resolve("pagerank.csv");
        Path workDir = tempDir.resolve("sparse-work");
        Files.writeString(input, "from,to\n100,1000000\n");

        AppConfig config = config(input, output, workDir, 4, 2);
        GraphPreprocessor.PreprocessingResult result = new GraphPreprocessor(config, new ProgressLogger()).preprocess();

        assertEquals(2, result.vertexCount());
        try (DiskIntArray vertices = new DiskIntArray(result.verticesPath(), result.vertexCount(), result.chunkSize())) {
            assertEquals(100, vertices.getInt(0));
            assertEquals(1_000_000, vertices.getInt(1));
        }
    }

    @Test
    void csvReaderUsesFirstTwoColumnsAndIgnoresExtraColumns() throws IOException {
        Path input = tempDir.resolve("extra-columns.csv");
        Path output = tempDir.resolve("output").resolve("pagerank.csv");
        Path workDir = tempDir.resolve("extra-columns-work");
        Files.writeString(input, """
                from,to,weight,comment
                1,2,10,abc
                2,3,20,def
                """);

        AppConfig config = config(input, output, workDir, 4, 2);
        GraphPreprocessor.PreprocessingResult result = new GraphPreprocessor(config, new ProgressLogger()).preprocess();

        assertEquals(3, result.vertexCount());
        assertEquals(2, result.edgeCount());
        try (DiskIntArray vertices = new DiskIntArray(result.verticesPath(), result.vertexCount(), result.chunkSize())) {
            assertEquals(1, vertices.getInt(0));
            assertEquals(2, vertices.getInt(1));
            assertEquals(3, vertices.getInt(2));
        }
    }

    @Test
    void supportsNegativeOriginalVertexIds() throws IOException {
        Path input = tempDir.resolve("edges.csv");
        Files.writeString(input, "from,to\n-1,2\n");

        AppConfig config = config(input, tempDir.resolve("out.csv"), tempDir.resolve("work"), 4, 2);
        GraphPreprocessor.PreprocessingResult result = new GraphPreprocessor(config, new ProgressLogger()).preprocess();

        assertEquals(2, result.vertexCount());
        try (DiskIntArray vertices = new DiskIntArray(result.verticesPath(), result.vertexCount(), result.chunkSize())) {
            assertEquals(-1, vertices.getInt(0));
            assertEquals(2, vertices.getInt(1));
        }
    }

    @Test
    void rejectsEmptyGraph() throws IOException {
        Path input = tempDir.resolve("edges.csv");
        Files.writeString(input, "from,to\n");

        AppConfig config = config(input, tempDir.resolve("out.csv"), tempDir.resolve("work"), 4, 2);

        assertThrows(IllegalArgumentException.class,
                () -> new GraphPreprocessor(config, new ProgressLogger()).preprocess());
    }

    private static AppConfig config(Path input, Path output, Path workDir, int chunkSize, int threads) {
        return new AppConfig(
                input,
                output,
                workDir,
                chunkSize,
                threads,
                0.85,
                30,
                1e-8,
                AppConfig.IdMode.EXTERNAL_DENSE,
                0,
                8,
                16L * 1024L * 1024L,
                false
        );
    }
}
