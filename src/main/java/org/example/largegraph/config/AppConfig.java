package org.example.largegraph.config;

import java.nio.file.Path;

public record AppConfig(
        Path input,
        Path output,
        Path workDir,
        int partitions,
        int threads,
        double damping,
        int maxIterations,
        double epsilon,
        IdMode idMode,
        boolean sortByRank
) {
    public AppConfig {
        if (input == null) {
            throw new IllegalArgumentException("--input is required");
        }
        if (output == null) {
            throw new IllegalArgumentException("--output is required");
        }
        if (workDir == null) {
            throw new IllegalArgumentException("--workdir is required");
        }
        if (partitions <= 0) {
            throw new IllegalArgumentException("--partitions must be positive");
        }
        if (threads <= 0) {
            throw new IllegalArgumentException("--threads must be positive");
        }
        if (damping <= 0.0 || damping >= 1.0) {
            throw new IllegalArgumentException("--damping must be in (0, 1)");
        }
        if (maxIterations <= 0) {
            throw new IllegalArgumentException("--max-iterations must be positive");
        }
        if (epsilon <= 0.0 || Double.isNaN(epsilon) || Double.isInfinite(epsilon)) {
            throw new IllegalArgumentException("--epsilon must be a positive finite number");
        }
        if (idMode == null) {
            throw new IllegalArgumentException("--id-mode is required");
        }
    }

    public enum IdMode {
        CONTIGUOUS;

        public static IdMode fromCliValue(String value) {
            if ("contiguous".equalsIgnoreCase(value)) {
                return CONTIGUOUS;
            }
            throw new IllegalArgumentException("unsupported --id-mode: " + value);
        }
    }
}
