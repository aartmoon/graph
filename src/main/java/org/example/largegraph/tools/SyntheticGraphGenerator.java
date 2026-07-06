package org.example.largegraph.tools;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public final class SyntheticGraphGenerator {
    private SyntheticGraphGenerator() {
    }

    public static void main(String[] args) throws IOException {
        Options options = Options.parse(args);
        if (options.output().getParent() != null) {
            Files.createDirectories(options.output().getParent());
        }

        Random random = new Random(options.seed());
        try (BufferedWriter writer = Files.newBufferedWriter(options.output(), StandardCharsets.UTF_8)) {
            writer.write("from,to");
            writer.newLine();

            long written = 0;
            if (options.hyperNode()) {
                long hyperEdges = Math.min(options.edges(), Math.max(0, options.vertices() - 1L));
                for (long to = 1; to <= hyperEdges; to++) {
                    writer.write("0,");
                    writer.write(Long.toString(to));
                    writer.newLine();
                    written++;
                }
            }

            while (written < options.edges()) {
                long from = random.nextLong(options.vertices());
                long to = random.nextLong(options.vertices());
                writer.write(Long.toString(from));
                writer.write(',');
                writer.write(Long.toString(to));
                writer.newLine();
                written++;
            }
        }
    }

    private record Options(long vertices, long edges, Path output, long seed, boolean hyperNode) {
        private static Options parse(String[] args) {
            if (args.length % 2 != 0) {
                throw new IllegalArgumentException("arguments must be --key value pairs");
            }
            Map<String, String> values = new HashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                values.put(args[i], args[i + 1]);
            }
            long vertices = parsePositiveLong(values, "--vertices");
            long edges = parsePositiveLong(values, "--edges");
            if (vertices > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("--vertices must fit int32 contiguous ids");
            }
            return new Options(
                    vertices,
                    edges,
                    Path.of(required(values, "--output")),
                    Long.parseLong(values.getOrDefault("--seed", "42")),
                    Boolean.parseBoolean(values.getOrDefault("--hyper-node", "false"))
            );
        }

        private static long parsePositiveLong(Map<String, String> values, String option) {
            long parsed = Long.parseLong(required(values, option));
            if (parsed <= 0) {
                throw new IllegalArgumentException(option + " must be positive");
            }
            return parsed;
        }

        private static String required(Map<String, String> values, String option) {
            String value = values.get(option);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("missing required option: " + option);
            }
            return value;
        }
    }
}
