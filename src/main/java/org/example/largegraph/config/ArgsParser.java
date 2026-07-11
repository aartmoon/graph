package org.example.largegraph.config;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class ArgsParser {
    private static final Set<String> KNOWN_OPTIONS = Set.of(
            "--input",
            "--output",
            "--workdir",
            "--chunk-size",
            "--threads",
            "--damping",
            "--max-iterations",
            "--epsilon",
            "--scatter-slice-mb"
    );

    private static final double DEFAULT_DAMPING = 0.85;
    private static final int DEFAULT_MAX_ITERATIONS = 200;
    private static final double DEFAULT_EPSILON = 1e-8;
    private static final int DEFAULT_SCATTER_SLICE_MB = 16;

    private ArgsParser() {
    }

    public static AppConfig parse(String[] args) {
        Map<String, String> values = parsePairs(args);
        requireRequired(values);

        return new AppConfig(
                Path.of(values.get("--input")),
                Path.of(values.get("--output")),
                Path.of(values.get("--workdir")),
                parsePositiveInt(values, "--chunk-size"),
                parsePositiveInt(values, "--threads"),
                parseDouble(values, "--damping", DEFAULT_DAMPING),
                parsePositiveInt(values, "--max-iterations", DEFAULT_MAX_ITERATIONS),
                parseDouble(values, "--epsilon", DEFAULT_EPSILON),
                scatterSliceBytes(values)
        );
    }

    public static String usage() {
        return """
                Usage:
                  java -jar largegraph-pagerank.jar \\
                    --input data/edges.csv \\
                    --output output/pagerank.csv \\
                    --workdir work \\
                    --chunk-size 100000 \\
                    --threads 8 \\
                    --damping 0.85 \\
                    --max-iterations 200 \\
                    --epsilon 1e-8 \\
                    --scatter-slice-mb 16
                """;
    }

    private static Map<String, String> parsePairs(String[] args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("arguments must be passed as --key value pairs");
        }

        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String key = args[i];
            String value = args[i + 1];
            if (!KNOWN_OPTIONS.contains(key)) {
                throw new IllegalArgumentException("unknown option: " + key);
            }
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("empty value for option: " + key);
            }
            if (values.put(key, value) != null) {
                throw new IllegalArgumentException("duplicate option: " + key);
            }
        }
        return values;
    }

    private static void requireRequired(Map<String, String> values) {
        for (String option : Set.of("--input", "--output", "--workdir", "--chunk-size", "--threads")) {
            if (!values.containsKey(option)) {
                throw new IllegalArgumentException("missing required option: " + option);
            }
        }
    }

    private static int parsePositiveInt(Map<String, String> values, String option) {
        return parsePositiveInt(values, option, null);
    }

    private static int parsePositiveInt(Map<String, String> values, String option, Integer defaultValue) {
        if (!values.containsKey(option)) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(values.get(option));
            if (parsed <= 0) {
                throw new IllegalArgumentException(option + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(option + " must be an integer", ex);
        }
    }

    private static double parseDouble(Map<String, String> values, String option, double defaultValue) {
        if (!values.containsKey(option)) {
            return defaultValue;
        }
        try {
            double parsed = Double.parseDouble(values.get(option));
            if (Double.isNaN(parsed) || Double.isInfinite(parsed)) {
                throw new IllegalArgumentException(option + " must be finite");
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(option + " must be a number", ex);
        }
    }

    private static long scatterSliceBytes(Map<String, String> values) {
        int megabytes = parsePositiveInt(values, "--scatter-slice-mb", DEFAULT_SCATTER_SLICE_MB);
        return Math.multiplyExact((long) megabytes, 1024L * 1024L);
    }
}
