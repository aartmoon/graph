package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.graph.GraphPreprocessor;
import org.example.largegraph.pagerank.PageRankEngine.PageRankRunResult;
import org.example.largegraph.util.MemoryUtils;
import org.example.largegraph.util.ProgressLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PageRankEngineTest {
    @TempDir
    Path tempDir;

    @Test
    void cycleGraphConvergesToEqualRanks() throws IOException {
        Path input = tempDir.resolve("cycle.csv");
        Files.writeString(input, """
                from,to
                1,2
                2,3
                3,1
                """);

        AppConfig config = config(input, tempDir.resolve("cycle-out.csv"), tempDir.resolve("cycle-work"), false, 30);
        GraphPreprocessor.PreprocessingResult graph = new GraphPreprocessor(config, new ProgressLogger()).preprocess();
        PageRankRunResult result = new PageRankEngine(config, new ProgressLogger()).run(graph);

        assertEquals(1.0, sum(result.ranks()), 1e-12);
        assertEquals(1.0 / 3.0, result.ranks()[0], 1e-12);
        assertEquals(1.0 / 3.0, result.ranks()[1], 1e-12);
        assertEquals(1.0 / 3.0, result.ranks()[2], 1e-12);
    }

    @Test
    void danglingNodeKeepsRankMassAndReceivesRank() throws IOException {
        Path input = tempDir.resolve("edges.csv");
        Files.writeString(input, "from,to\n1,2\n");

        AppConfig config = config(input, tempDir.resolve("pagerank.csv"), tempDir.resolve("work"), false, 30);
        GraphPreprocessor.PreprocessingResult graph = new GraphPreprocessor(config, new ProgressLogger()).preprocess();
        PageRankRunResult result = new PageRankEngine(config, new ProgressLogger()).run(graph);

        assertEquals(1.0, sum(result.ranks()), 1e-12);
        assertTrue(result.ranks()[1] > result.ranks()[0]);
        assertTrue(result.iterations() <= config.maxIterations());
    }

    @Test
    void graphFromTaskCompletesAndWritesCsv() throws IOException {
        Path input = tempDir.resolve("task-graph.csv");
        Path output = tempDir.resolve("task-output.csv");
        Files.writeString(input, """
                from,to
                1,2
                2,3
                3,1
                4,1
                """);

        AppConfig config = config(input, output, tempDir.resolve("task-work"), false, 30);
        GraphPreprocessor.PreprocessingResult graph = new GraphPreprocessor(config, new ProgressLogger()).preprocess();
        PageRankRunResult result = new PageRankEngine(config, new ProgressLogger()).run(graph);
        new PageRankResultWriter(config).write(result, graph.vertexIndexer());

        assertEquals(4, graph.vertexCount());
        assertEquals(4, graph.edgeCount());
        assertEquals(1.0, sum(result.ranks()), 1e-12);
        assertTrue(Files.readString(output).startsWith("vertex,rank\n"));
    }

    @Test
    void hyperNodeSmokeTestDoesNotNeedAdjacencyList() throws IOException {
        Path input = tempDir.resolve("hyper-node.csv");
        Path output = tempDir.resolve("hyper-output.csv");
        StringBuilder csv = new StringBuilder("from,to\n");
        for (int to = 2; to <= 10_000; to++) {
            csv.append("1,").append(to).append('\n');
        }
        Files.writeString(input, csv);

        AppConfig config = config(input, output, tempDir.resolve("hyper-work"), false, 2);
        GraphPreprocessor.PreprocessingResult graph = new GraphPreprocessor(config, new ProgressLogger()).preprocess();
        PageRankRunResult result = new PageRankEngine(config, new ProgressLogger()).run(graph);
        new PageRankResultWriter(config).write(result, graph.vertexIndexer());

        System.gc();
        assertEquals(10_000, graph.vertexCount());
        assertEquals(9_999, graph.edgeCount());
        assertEquals(1.0, sum(result.ranks()), 1e-12);
        assertTrue(Files.exists(output));
        assertTrue(MemoryUtils.usedHeapBytes() < 128L * 1024L * 1024L);
    }

    @Test
    void writesCsvSortedByVertexByDefault() throws IOException {
        Path input = tempDir.resolve("edges.csv");
        Path output = tempDir.resolve("pagerank.csv");
        Files.writeString(input, """
                from,to
                30,10
                10,20
                20,30
                """);

        AppConfig config = config(input, output, tempDir.resolve("work"), false, 1);
        GraphPreprocessor.PreprocessingResult graph = new GraphPreprocessor(config, new ProgressLogger()).preprocess();
        PageRankRunResult result = new PageRankEngine(config, new ProgressLogger()).run(graph);
        new PageRankResultWriter(config).write(result, graph.vertexIndexer());

        assertEquals("""
                vertex,rank
                10,0.3333333333333333
                20,0.3333333333333333
                30,0.3333333333333333
                """, Files.readString(output));
    }

    private static AppConfig config(Path input, Path output, Path workDir, boolean sortByRank, int maxIterations) {
        return new AppConfig(
                input,
                output,
                workDir,
                16,
                2,
                0.85,
                maxIterations,
                1e-12,
                AppConfig.IdMode.CONTIGUOUS,
                sortByRank
        );
    }

    private static double sum(double[] values) {
        double sum = 0.0;
        for (double value : values) {
            sum += value;
        }
        return sum;
    }
}
