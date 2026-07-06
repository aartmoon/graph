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
    private static final int MAX_INT_SORT_CHUNK = 500_000;
    private static final int MAX_RECORD_SORT_CHUNK = 250_000;

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
        long pagerankChunkEstimate = safeMultiply(safeMultiply(config.chunkSize(), 64L), activeTasks);
        long intSortChunkEstimate = safeMultiply(Math.min(config.chunkSize(), MAX_INT_SORT_CHUNK), Integer.BYTES);
        long recordSortChunkEstimate = safeMultiply(Math.min(config.chunkSize(), MAX_RECORD_SORT_CHUNK), 32L);
        long maxHeap = MemoryUtils.maxHeapBytes();
        long largestEstimate = Math.max(pagerankChunkEstimate, Math.max(intSortChunkEstimate, recordSortChunkEstimate));
        if (largestEstimate > maxHeap / 2) {
            logger.info("""
                    WARNING memory configuration may be risky:
                      maxHeap=%s
                      pagerankChunkEstimate=%s
                      intSortChunkEstimate=%s
                      recordSortChunkEstimate=%s
                      consider --chunk-size 10000..100000 for -Xmx128m"""
                    .formatted(
                            MemoryUtils.humanReadableBytes(maxHeap),
                            MemoryUtils.humanReadableBytes(pagerankChunkEstimate),
                            MemoryUtils.humanReadableBytes(intSortChunkEstimate),
                            MemoryUtils.humanReadableBytes(recordSortChunkEstimate)
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
