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
        IdMode idMode,
        int topK,
        int gatherChunkCacheSize,
        long scatterSliceBytes,
        boolean keepMessages
) {
    public static final int MAX_TOP_K = 1_000_000;

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
        if (topK < 0) {
            throw new IllegalArgumentException("--top-k must be non-negative");
        }
        if (topK > MAX_TOP_K) {
            throw new IllegalArgumentException("--top-k must be <= " + MAX_TOP_K);
        }
        if (gatherChunkCacheSize <= 0) {
            throw new IllegalArgumentException("--gather-chunk-cache-size must be positive");
        }
        if (scatterSliceBytes <= 0) {
            throw new IllegalArgumentException("--scatter-slice-mb must be positive");
        }
        validateBufferSize("--chunk-size for double chunks", chunkSize, Double.BYTES);
        validateBufferSize("--chunk-size for int chunks", chunkSize, Integer.BYTES);
        validateBufferSize("--scatter-slice-mb", scatterSliceBytes, 1);
        validateGatherMemoryShape(chunkSize, gatherChunkCacheSize, threads);
    }

    private static void validateBufferSize(String option, long itemCount, int bytesPerItem) {
        long bytes = Math.multiplyExact(itemCount, bytesPerItem);
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(option + " is too large: required buffer bytes=" + bytes);
        }
    }

    private static void validateGatherMemoryShape(int chunkSize, int gatherChunkCacheSize, int threads) {
        long perChunkBytes = Math.multiplyExact((long) chunkSize, Double.BYTES * 2L);
        long cacheBytes = Math.multiplyExact(perChunkBytes, gatherChunkCacheSize);
        long totalBytes = Math.multiplyExact(cacheBytes, threads);
        long maxHeap = Runtime.getRuntime().maxMemory();
        if (totalBytes > maxHeap * 3L / 4L) {
            throw new IllegalArgumentException(
                    "gather cache configuration is too large for max heap: estimated=%d maxHeap=%d"
                            .formatted(totalBytes, maxHeap)
            );
        }
    }

    public enum IdMode {
        EXTERNAL_DENSE;

        public static IdMode fromCliValue(String value) {
            if ("external-dense".equalsIgnoreCase(value) || "contiguous".equalsIgnoreCase(value)) {
                return EXTERNAL_DENSE;
            }
            throw new IllegalArgumentException("unsupported --id-mode: " + value);
        }
    }
}
