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
import org.example.largegraph.util.PathLayoutValidator;
import org.example.largegraph.util.ProgressLogger;
import org.example.largegraph.util.SaturatedMath;

import java.io.IOException;

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
            PathLayoutValidator.validate(config);
            try (WorkDirSession workspace = WorkDirSession.acquire(config.workDir())) {
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
        long preprocessingPeak = SaturatedMath.multiply(config.chunkSize(), 16L);
        long scatterPeak = SaturatedMath.multiply(config.chunkSize(), 24L);
        long required = Math.max(preprocessingPeak, scatterPeak);
        if (required > usableHeap) {
            throw new IllegalArgumentException("--chunk-size is too large for heap: required=%s usable=%s"
                    .formatted(
                            MemoryUtils.humanReadableBytes(required),
                            MemoryUtils.humanReadableBytes(usableHeap)
                    ));
        }
    }

}
