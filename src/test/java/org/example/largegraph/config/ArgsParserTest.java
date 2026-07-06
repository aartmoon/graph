package org.example.largegraph.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ArgsParserTest {
    @Test
    void parsesValidArguments() {
        AppConfig config = ArgsParser.parse(validArgs());

        assertEquals("data/edges.csv", config.input().toString());
        assertEquals("output/pagerank.csv", config.output().toString());
        assertEquals("work", config.workDir().toString());
        assertEquals(1_000_000, config.chunkSize());
        assertEquals(8, config.threads());
        assertEquals(0.85, config.damping());
        assertEquals(200, config.maxIterations());
        assertEquals(1e-8, config.epsilon());
        assertEquals(AppConfig.IdMode.CONTIGUOUS, config.idMode());
        assertEquals(0, config.topK());
        assertFalse(config.keepMessages());
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
        assertEquals(AppConfig.IdMode.CONTIGUOUS, config.idMode());
        assertEquals(0, config.topK());
        assertFalse(config.keepMessages());
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

    private static String[] validArgs() {
        return new String[]{
                "--input", "data/edges.csv",
                "--output", "output/pagerank.csv",
                "--workdir", "work",
                "--chunk-size", "1000000",
                "--threads", "8",
                "--damping", "0.85",
                "--max-iterations", "200",
                "--epsilon", "1e-8",
                "--id-mode", "contiguous",
                "--top-k", "0",
                "--keep-messages", "false"
        };
    }
}
