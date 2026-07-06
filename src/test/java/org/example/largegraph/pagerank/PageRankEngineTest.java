package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.graph.GraphPreprocessor;
import org.example.largegraph.io.MessagePartitionWriterManager;
import org.example.largegraph.storage.DiskDoubleArray;
import org.example.largegraph.util.MemoryUtils;
import org.example.largegraph.util.ProgressLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PageRankEngineTest {
    @TempDir
    Path tempDir;

    @Test
    void cycleGraphHasEqualRanksForCycleVertices() throws IOException {
        Path input = tempDir.resolve("cycle.csv");
        Files.writeString(input, """
                from,to
                1,2
                2,3
                3,1
                """);

        RunResult run = run(input, tempDir.resolve("cycle"), 2, 50);
        double[] ranks = readAllRanks(run.result());

        assertEquals(1.0, sumRanksStreaming(run.result()), 1e-9);
        assertEquals(ranks[0], ranks[1], 1e-9);
        assertEquals(ranks[1], ranks[2], 1e-9);
    }

    @Test
    void danglingNodeKeepsRankMass() throws IOException {
        Path input = tempDir.resolve("dangling.csv");
        Files.writeString(input, "from,to\n1,2\n");

        RunResult run = run(input, tempDir.resolve("dangling"), 2, 50);
        double[] ranks = readAllRanks(run.result());

        assertEquals(1.0, sumRanksStreaming(run.result()), 1e-9);
        assertTrue(ranks[1] > ranks[0]);
    }

    @Test
    void taskSampleCompletesAndWritesCsv() throws IOException {
        Path input = tempDir.resolve("task-graph.csv");
        Files.writeString(input, """
                from,to
                1,2
                2,3
                3,1
                4,1
                """);

        Path output = tempDir.resolve("task-output.csv");
        RunResult run = run(input, tempDir.resolve("task"), output, 2, 50);
        new PageRankResultWriter(run.config()).write(run.result());

        String csv = Files.readString(output);
        assertTrue(csv.startsWith("vertex,rank\n"));
        assertTrue(csv.contains("1,"));
        assertTrue(csv.contains("2,"));
        assertTrue(csv.contains("3,"));
        assertTrue(csv.contains("4,"));
        assertTrue(csv.lines().noneMatch(line -> line.startsWith("0,")));
    }

    @Test
    void sparseIdsOutputOnlyOriginalObservedVertices() throws IOException {
        Path input = tempDir.resolve("sparse.csv");
        Files.writeString(input, "from,to\n100,1000000\n");

        Path output = tempDir.resolve("sparse-output.csv");
        RunResult run = run(input, tempDir.resolve("sparse"), output, 2, 20);
        new PageRankResultWriter(run.config()).write(run.result());

        List<String> lines = Files.readAllLines(output);
        assertEquals(3, lines.size());
        assertEquals("100", lines.get(1).split(",")[0]);
        assertEquals("1000000", lines.get(2).split(",")[0]);
    }

    @Test
    void hyperNodeSmokeTestCompletesWithoutAdjacencyList() throws IOException {
        Path input = tempDir.resolve("hyper-node.csv");
        StringBuilder csv = new StringBuilder("from,to\n");
        for (int to = 2; to <= 10_000; to++) {
            csv.append("1,").append(to).append('\n');
        }
        Files.writeString(input, csv);

        RunResult run = run(input, tempDir.resolve("hyper"), 1_024, 3);

        System.gc();
        assertEquals(10_000, run.result().vertexCount());
        assertEquals(1.0, sumRanksStreaming(run.result()), 1e-9);
        assertTrue(MemoryUtils.usedHeapBytes() < 128L * 1024L * 1024L);
    }

    @Test
    void chunkBoundaryGraphHasNoOffsetBugs() throws IOException {
        Path input = tempDir.resolve("boundary.csv");
        Files.writeString(input, """
                from,to
                3,4
                4,5
                5,3
                """);

        Path output = tempDir.resolve("boundary-output.csv");
        RunResult run = run(input, tempDir.resolve("boundary"), output, 4, 30);
        new PageRankResultWriter(run.config()).write(run.result());

        assertEquals(3, run.result().vertexCount());
        assertEquals(1.0, sumRanksStreaming(run.result()), 1e-9);
        assertTrue(Files.readString(output).contains("5,"));
    }

    @Test
    void messageManifestContainsOnlyTouchedPartitionsSorted() throws IOException {
        Path workerDir = tempDir.resolve("messages").resolve("worker-00000");
        try (MessagePartitionWriterManager writers = new MessagePartitionWriterManager(workerDir, 4, 2)) {
            writers.write(2, 5, 0.25);
            writers.write(0, 1, 0.75);
            writers.write(2, 6, 0.5);
        }

        assertEquals(List.of("0", "2"), Files.readAllLines(workerDir.resolve("manifest.txt")));
        assertTrue(Files.exists(workerDir.resolve("msg-bucket-00000.bin")));
        assertTrue(Files.exists(workerDir.resolve("msg-bucket-00002.bin")));
        assertTrue(Files.notExists(workerDir.resolve("msg-bucket-00001.bin")));
        assertTrue(Files.notExists(workerDir.resolve("msg-bucket-00003.bin")));
    }

    @Test
    void repeatedRunsAreDeterministic() throws IOException {
        Path input = tempDir.resolve("deterministic.csv");
        Files.writeString(input, """
                from,to
                1,2
                2,3
                3,1
                4,1
                """);

        Path outputA = tempDir.resolve("a.csv");
        Path outputB = tempDir.resolve("b.csv");
        RunResult runA = run(input, tempDir.resolve("work-a"), outputA, 2, 20);
        RunResult runB = run(input, tempDir.resolve("work-b"), outputB, 2, 20);
        new PageRankResultWriter(runA.config()).write(runA.result());
        new PageRankResultWriter(runB.config()).write(runB.result());

        assertEquals(Files.readString(outputA), Files.readString(outputB));
    }

    private RunResult run(Path input, Path workDir, int chunkSize, int maxIterations) throws IOException {
        return run(input, workDir, tempDir.resolve("out-" + workDir.getFileName() + ".csv"), chunkSize, maxIterations);
    }

    private RunResult run(Path input, Path workDir, Path output, int chunkSize, int maxIterations) throws IOException {
        AppConfig config = new AppConfig(
                input,
                output,
                workDir,
                chunkSize,
                4,
                0.85,
                maxIterations,
                1e-10,
                AppConfig.IdMode.CONTIGUOUS,
                0,
                false
        );
        GraphPreprocessor.PreprocessingResult graph = new GraphPreprocessor(config, new ProgressLogger()).preprocess();
        PageRankEngine.PageRankRunResult result = new PageRankEngine(config, new ProgressLogger()).run(graph);
        return new RunResult(config, result);
    }

    private static double[] readAllRanks(PageRankEngine.PageRankRunResult result) throws IOException {
        try (DiskDoubleArray ranks = new DiskDoubleArray(result.rankPath(), result.vertexCount(), 1_024)) {
            return ranks.readChunk(0, Math.toIntExact(result.vertexCount()));
        }
    }

    private static double sumRanksStreaming(PageRankEngine.PageRankRunResult result) throws IOException {
        double sum = 0.0;
        try (DiskDoubleArray ranks = new DiskDoubleArray(result.rankPath(), result.vertexCount(), 1_024, false)) {
            for (long start = 0; start < result.vertexCount(); start += 1_024) {
                int length = (int) Math.min(1_024, result.vertexCount() - start);
                double[] chunk = ranks.readChunk(start, length);
                for (double value : chunk) {
                    sum += value;
                }
            }
        }
        return sum;
    }

    private record RunResult(AppConfig config, PageRankEngine.PageRankRunResult result) {
    }
}
