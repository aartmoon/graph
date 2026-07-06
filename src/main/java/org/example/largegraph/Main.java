package org.example.largegraph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.config.ArgsParser;
import org.example.largegraph.graph.GraphPreprocessor;
import org.example.largegraph.graph.GraphPreprocessor.PreprocessingResult;
import org.example.largegraph.pagerank.PageRankEngine;
import org.example.largegraph.pagerank.PageRankEngine.PageRankRunResult;
import org.example.largegraph.pagerank.PageRankResultWriter;
import org.example.largegraph.util.MemoryUtils;
import org.example.largegraph.util.ProgressLogger;

import java.io.IOException;
import java.nio.file.Files;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) {
        try {
            AppConfig config = ArgsParser.parse(args);
            prepareDirectories(config);

            ProgressLogger logger = new ProgressLogger();
            warnIfChunkSizeLooksRisky(config, logger);
            logger.info("Starting preprocessing");
            PreprocessingResult graph = new GraphPreprocessor(config, logger).preprocess();

            logger.info("Starting PageRank engine");
            PageRankRunResult result = new PageRankEngine(config, logger).run(graph);

            logger.info("Writing result CSV");
            new PageRankResultWriter(config).write(result);
            logger.info("Done");
        } catch (IllegalArgumentException ex) {
            System.err.println("Invalid arguments: " + ex.getMessage());
            System.err.println(ArgsParser.usage());
            System.exit(2);
        } catch (IOException ex) {
            System.err.println("I/O error: " + ex.getMessage());
            System.exit(1);
        } catch (RuntimeException ex) {
            System.err.println("Unexpected error: " + ex.getMessage());
            System.exit(1);
        }
    }

    private static void warnIfChunkSizeLooksRisky(AppConfig config, ProgressLogger logger) {
        long activeTasks = Math.max(1, config.threads());
        long estimatedChunkBytes = safeMultiply(config.chunkSize(), 64L);
        long estimatedConcurrentBytes = safeMultiply(estimatedChunkBytes, activeTasks);
        long maxHeap = MemoryUtils.maxHeapBytes();
        if (estimatedConcurrentBytes > maxHeap / 2) {
            logger.info("WARNING chunk-size may be too large for heap: chunkSize=%d threads=%d estimatedChunkHeap=%s maxHeap=%s"
                    .formatted(
                            config.chunkSize(),
                            config.threads(),
                            MemoryUtils.humanReadableBytes(estimatedConcurrentBytes),
                            MemoryUtils.humanReadableBytes(maxHeap)
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

    private static void prepareDirectories(AppConfig config) throws IOException {
        try {
            Files.createDirectories(config.workDir());
        } catch (IOException ex) {
            throw new IOException("cannot create workdir: " + config.workDir(), ex);
        }
        if (!Files.isDirectory(config.workDir())) {
            throw new IOException("workdir path is not a directory: " + config.workDir());
        }
        if (config.output().getParent() != null) {
            Files.createDirectories(config.output().getParent());
        }
    }
}
