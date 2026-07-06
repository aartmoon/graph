package org.example.largegraph.graph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.util.ProgressLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GraphPreprocessorTest {
    @TempDir
    Path tempDir;

    @Test
    void createsPreprocessingFilesWithoutKeepingEdgesInMemory() throws IOException {
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

        AppConfig config = config(input, output, workDir, 4);
        GraphPreprocessor.PreprocessingResult result = new GraphPreprocessor(config, new ProgressLogger()).preprocess();

        assertEquals(3, result.vertexCount());
        assertEquals(4, result.edgeCount());
        assertTrue(Files.exists(workDir.resolve("out_degree.bin")));
        assertTrue(Files.exists(workDir.resolve("vertex_ids.bin")));
        assertTrue(Files.exists(workDir.resolve("current_rank.bin")));
        assertTrue(Files.exists(workDir.resolve("next_rank.bin")));

        for (int partition = 0; partition < 4; partition++) {
            assertTrue(Files.exists(workDir.resolve("partitions").resolve("part-%05d.bin".formatted(partition))));
        }

        assertArrayEquals(new int[]{2, 1, 1}, readInts(workDir.resolve("out_degree.bin"), 3));
        assertArrayEquals(new int[]{10, 20, 30}, readInts(workDir.resolve("vertex_ids.bin"), 3));
    }

    @Test
    void rejectsNegativeVertexIdsInContiguousMode() throws IOException {
        Path input = tempDir.resolve("edges.csv");
        Files.writeString(input, "from,to\n-1,2\n");

        AppConfig config = config(input, tempDir.resolve("out.csv"), tempDir.resolve("work"), 2);

        assertThrows(IllegalArgumentException.class,
                () -> new GraphPreprocessor(config, new ProgressLogger()).preprocess());
    }

    @Test
    void rejectsEmptyGraph() throws IOException {
        Path input = tempDir.resolve("edges.csv");
        Files.writeString(input, "from,to\n");

        AppConfig config = config(input, tempDir.resolve("out.csv"), tempDir.resolve("work"), 2);

        assertThrows(IllegalArgumentException.class,
                () -> new GraphPreprocessor(config, new ProgressLogger()).preprocess());
    }

    private static AppConfig config(Path input, Path output, Path workDir, int partitions) {
        return new AppConfig(
                input,
                output,
                workDir,
                partitions,
                2,
                0.85,
                30,
                1e-8,
                AppConfig.IdMode.CONTIGUOUS,
                false
        );
    }

    private static int[] readInts(Path path, int count) throws IOException {
        int[] values = new int[count];
        try (DataInputStream input = new DataInputStream(Files.newInputStream(path))) {
            for (int i = 0; i < count; i++) {
                values[i] = input.readInt();
            }
        }
        return values;
    }
}
