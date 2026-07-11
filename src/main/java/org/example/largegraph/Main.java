package org.example.largegraph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.config.ArgsParser;
import org.example.largegraph.graph.GraphPreprocessor;
import org.example.largegraph.graph.GraphPreprocessor.PreprocessingResult;
import org.example.largegraph.graph.WorkDirSession;
import org.example.largegraph.pagerank.PageRankEngine;
import org.example.largegraph.pagerank.PageRankEngine.PageRankRunResult;
import org.example.largegraph.pagerank.PageRankResultWriter;
import org.example.largegraph.util.MemoryUtils;
import org.example.largegraph.util.ProgressLogger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        AppConfig config;
        try {
            config = ArgsParser.parse(args);
        } catch (IllegalArgumentException ex) {
            System.err.println("Invalid arguments: " + ex.getMessage());
            System.err.println(ArgsParser.usage());
            System.exit(2);
            return;
        }

        try {
            validateChunkAgainstHeap(config);
        } catch (IllegalArgumentException ex) {
            System.err.println("Invalid arguments: " + ex.getMessage());
            System.err.println(ArgsParser.usage());
            System.exit(2);
            return;
        }

        try {
            ProgressLogger logger = new ProgressLogger();
            try (WorkDirSession workspace = WorkDirSession.acquire(config.workDir())) {
                validatePathLayout(config);
                workspace.prepare();

                logger.info("Starting preprocessing");
                PreprocessingResult graph = new GraphPreprocessor(config, logger).preprocess();

                logger.info("Starting PageRank engine");
                PageRankRunResult result = new PageRankEngine(config, logger).run(graph);

                logger.info("Writing result CSV");
                new PageRankResultWriter(config).write(result);
                workspace.cleanupAfterSuccess();
                logger.info("Done");
            }
        } catch (IOException ex) {
            System.err.println("I/O error: " + ex.getMessage());
            System.exit(1);
        } catch (RuntimeException ex) {
            System.err.println("Unexpected error: " + ex.getMessage());
            System.exit(1);
        }
    }

    private static void validateChunkAgainstHeap(AppConfig config) {
        long maxHeap = MemoryUtils.maxHeapBytes();
        long usableHeap = Math.max(1L, maxHeap * 60L / 100L);
        long preprocessingPeak = safeMultiply(config.chunkSize(), 16L);
        long scatterPeak = safeMultiply(config.chunkSize(), 24L);
        long required = Math.max(preprocessingPeak, scatterPeak);
        if (required > usableHeap) {
            throw new IllegalArgumentException("--chunk-size is too large for heap: required=%s usable=%s"
                    .formatted(
                            MemoryUtils.humanReadableBytes(required),
                            MemoryUtils.humanReadableBytes(usableHeap)
                    ));
        }
    }

    private static long safeMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

    private static void validatePathLayout(AppConfig config) throws IOException {
        if (!Files.exists(config.input())) {
            throw new IOException("input file does not exist: " + config.input());
        }
        if (!Files.isRegularFile(config.input())) {
            throw new IOException("input path is not a regular file: " + config.input());
        }
        Path input = config.input().toRealPath();
        Path work = config.workDir().toRealPath();
        Path output = resolvePossiblyMissingPath(config.output());
        if (input.equals(output)) {
            throw new IOException("input and output must be different files");
        }
        if (input.startsWith(work)) {
            throw new IOException("input must not be located inside workdir");
        }
        if (output.startsWith(work)) {
            throw new IOException("output must not be located inside workdir");
        }
    }

    private static Path resolvePossiblyMissingPath(Path path) throws IOException {
        if (Files.exists(path)) {
            return path.toRealPath();
        }
        Path absolute = path.toAbsolutePath().normalize();
        Path fileName = absolute.getFileName();
        Path parent = absolute.getParent();
        if (parent == null) {
            return absolute;
        }
        Path existingParent = parent;
        while (existingParent != null && !Files.exists(existingParent)) {
            existingParent = existingParent.getParent();
        }
        if (existingParent == null) {
            return absolute;
        }
        Path realParent = existingParent.toRealPath();
        Path relative = existingParent.relativize(parent);
        return realParent.resolve(relative).resolve(fileName).normalize();
    }
}
