package org.example.largegraph.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ArgsParserTest {
    @Test
    void parsesValidArguments() {
        AppConfig config = ArgsParser.parse(validArgs());

        assertEquals("data/edges.csv", config.input().toString());
        assertEquals("output/pagerank.csv", config.output().toString());
        assertEquals("work", config.workDir().toString());
        assertEquals(10_000, config.chunkSize());
        assertEquals(8, config.threads());
        assertEquals(0.85, config.damping());
        assertEquals(200, config.maxIterations());
        assertEquals(1e-8, config.epsilon());
        assertEquals(32L * 1024L * 1024L, config.scatterSliceBytes());
    }

    @Test
    void appliesOptionalDefaults() {
        AppConfig config = ArgsParser.parse(new String[]{
                "--input", "data/edges.csv",
                "--output", "output/pagerank.csv",
                "--workdir", "work",
                "--chunk-size", "1000",
                "--threads", "4"
        });

        assertEquals(0.85, config.damping());
        assertEquals(200, config.maxIterations());
        assertEquals(1e-8, config.epsilon());
        assertEquals(16L * 1024L * 1024L, config.scatterSliceBytes());
    }

    @Test
    void rejectsMissingRequiredArguments() {
        assertThrows(IllegalArgumentException.class, () -> ArgsParser.parse(new String[]{"--input", "data/edges.csv"}));
    }

    @Test
    void rejectsInvalidDamping() {
        String[] args = validArgs();
        args[11] = "1.0";

        assertThrows(IllegalArgumentException.class, () -> ArgsParser.parse(args));
    }

    @Test
    void rejectsUnknownRemovedOptions() {
        String[] args = validArgs();
        String[] withRemovedOption = java.util.Arrays.copyOf(args, args.length + 2);
        withRemovedOption[args.length] = "--top-k";
        withRemovedOption[args.length + 1] = "10";

        assertThrows(IllegalArgumentException.class, () -> ArgsParser.parse(withRemovedOption));
    }

    private static String[] validArgs() {
        return new String[]{
                "--input", "data/edges.csv",
                "--output", "output/pagerank.csv",
                "--workdir", "work",
                "--chunk-size", "10000",
                "--threads", "8",
                "--damping", "0.85",
                "--max-iterations", "200",
                "--epsilon", "1e-8",
                "--scatter-slice-mb", "32"
        };
    }
}
