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
            "--partitions",
            "--threads",
            "--damping",
            "--max-iterations",
            "--epsilon",
            "--id-mode",
            "--sort-by-rank"
    );

    private ArgsParser() {
    }

    public static AppConfig parse(String[] args) {
        Map<String, String> values = parsePairs(args);
        requireAll(values);

        return new AppConfig(
                Path.of(values.get("--input")),
                Path.of(values.get("--output")),
                Path.of(values.get("--workdir")),
                parsePositiveInt(values, "--partitions"),
                parsePositiveInt(values, "--threads"),
                parseDouble(values, "--damping"),
                parsePositiveInt(values, "--max-iterations"),
                parseDouble(values, "--epsilon"),
                AppConfig.IdMode.fromCliValue(values.get("--id-mode")),
                parseBoolean(values.get("--sort-by-rank"), "--sort-by-rank")
        );
    }

    public static String usage() {
        return """
                Usage:
                  java -jar largegraph-pagerank.jar \\
                    --input data/edges.csv \\
                    --output output/pagerank.csv \\
                    --workdir work \\
                    --partitions 64 \\
                    --threads 8 \\
                    --damping 0.85 \\
                    --max-iterations 30 \\
                    --epsilon 1e-8 \\
                    --id-mode contiguous \\
                    --sort-by-rank false
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

    private static void requireAll(Map<String, String> values) {
        for (String option : KNOWN_OPTIONS) {
            if (!values.containsKey(option)) {
                throw new IllegalArgumentException("missing required option: " + option);
            }
        }
    }

    private static int parsePositiveInt(Map<String, String> values, String option) {
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

    private static double parseDouble(Map<String, String> values, String option) {
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

    private static boolean parseBoolean(String value, String option) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(option + " must be true or false");
    }
}
