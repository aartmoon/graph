package org.example.largegraph.config;

import java.nio.file.Path;

public record AppConfig(
        Path input,
        Path output,
        Path workDir,
        int chunkSize,
        int threads,
        double damping,
        int maxIterations,
        double epsilon,
        long scatterSliceBytes
) {
    public static final int MAX_THREADS = 256;

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
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("--chunk-size must be positive");
        }
        if (threads <= 0) {
            throw new IllegalArgumentException("--threads must be positive");
        }
        if (threads > MAX_THREADS) {
            throw new IllegalArgumentException("--threads must be <= " + MAX_THREADS);
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
        if (scatterSliceBytes <= 0) {
            throw new IllegalArgumentException("--scatter-slice-mb must be positive");
        }
        validateBufferSize("--chunk-size for double chunks", chunkSize, Double.BYTES);
        validateBufferSize("--chunk-size for int chunks", chunkSize, Integer.BYTES);
        validateBufferSize("--scatter-slice-mb", scatterSliceBytes, 1);
    }

    private static void validateBufferSize(String option, long itemCount, int bytesPerItem) {
        long bytes = Math.multiplyExact(itemCount, bytesPerItem);
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(option + " is too large: required buffer bytes=" + bytes);
        }
    }

}
