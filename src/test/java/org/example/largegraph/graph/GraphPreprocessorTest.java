package org.example.largegraph.graph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.storage.DiskIntArray;
import org.example.largegraph.util.ProgressLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
        assertTrue(Files.exists(workDir.resolve("vertices.bin")));
        assertTrue(Files.exists(workDir.resolve("vertex").resolve("out_degree.bin")));
        assertTrue(Files.exists(workDir.resolve("vertex").resolve("rank_current.bin")));
        assertFalse(Files.exists(workDir.resolve("vertex").resolve("rank_next.bin")));
        assertTrue(Files.exists(workDir.resolve("edges_by_source").resolve("src-part-00000.bin")));
        assertFalse(Files.exists(workDir.resolve("raw_edges.bin")));
        assertFalse(Files.exists(workDir.resolve("endpoint_refs.bin")));
        assertFalse(Files.exists(workDir.resolve("endpoint_refs.sorted.bin")));
        assertFalse(Files.exists(workDir.resolve("endpoint_assignments.bin")));
        assertFalse(Files.exists(workDir.resolve("endpoint_assignments.sorted.bin")));
        assertFalse(Files.exists(workDir.resolve("dense_edges.bin")));
        assertFalse(Files.exists(workDir.resolve("dense_edges.sorted.bin")));
        assertFalse(Files.exists(workDir.resolve("sort")));

        try (DiskIntArray outDegree = new DiskIntArray(result.outDegreePath(), result.vertexCount(), result.chunkSize())) {
            assertArrayEquals(new int[]{2, 1, 1}, readAll(outDegree, result.vertexCount()));
        }
        try (DiskIntArray vertices = new DiskIntArray(result.verticesPath(), result.vertexCount(), result.chunkSize())) {
            assertArrayEquals(new int[]{10, 20, 30}, readAll(vertices, result.vertexCount()));
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
            assertArrayEquals(new int[]{2, 0, 0}, readAll(outDegree, result.vertexCount()));
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
            assertArrayEquals(new int[]{100, 1_000_000}, readAll(vertices, result.vertexCount()));
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
            assertArrayEquals(new int[]{1, 2, 3}, readAll(vertices, result.vertexCount()));
        }
    }

    @Test
    void csvReaderStripsUtf8BomFromHeader() throws IOException {
        Path input = tempDir.resolve("bom.csv");
        Path output = tempDir.resolve("output").resolve("pagerank.csv");
        Path workDir = tempDir.resolve("bom-work");
        Files.writeString(input, "\uFEFFfrom,to\n1,2\n");

        AppConfig config = config(input, output, workDir, 4, 2);
        GraphPreprocessor.PreprocessingResult result = new GraphPreprocessor(config, new ProgressLogger()).preprocess();

        assertEquals(2, result.vertexCount());
        assertEquals(1, result.edgeCount());
    }

    @Test
    void supportsNegativeOriginalVertexIds() throws IOException {
        Path input = tempDir.resolve("edges.csv");
        Files.writeString(input, "from,to\n-1,2\n");

        AppConfig config = config(input, tempDir.resolve("out.csv"), tempDir.resolve("work"), 4, 2);
        GraphPreprocessor.PreprocessingResult result = new GraphPreprocessor(config, new ProgressLogger()).preprocess();

        assertEquals(2, result.vertexCount());
        try (DiskIntArray vertices = new DiskIntArray(result.verticesPath(), result.vertexCount(), result.chunkSize())) {
            assertArrayEquals(new int[]{-1, 2}, readAll(vertices, result.vertexCount()));
        }
    }

    @Test
    void rejectsEmptyGraph() throws IOException {
        Path input = tempDir.resolve("edges.csv");
        Files.writeString(input, "from,to\n");

        AppConfig config = config(input, tempDir.resolve("out.csv"), tempDir.resolve("work"), 4, 2);

        assertThrows(IOException.class,
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
                16L * 1024L * 1024L
        );
    }

    private static int[] readAll(DiskIntArray array, long vertexCount) throws IOException {
        return array.readIntChunk(0, Math.toIntExact(vertexCount));
    }
}
