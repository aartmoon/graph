package org.example.largegraph;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.config.ArgsParser;
import org.example.largegraph.graph.GraphPreprocessor;
import org.example.largegraph.graph.GraphPreprocessor.PreprocessingResult;
import org.example.largegraph.pagerank.PageRankEngine;
import org.example.largegraph.pagerank.PageRankEngine.PageRankRunResult;
import org.example.largegraph.pagerank.PageRankResultWriter;
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

    private static void prepareDirectories(AppConfig config) throws IOException {
        try {
            Files.createDirectories(config.workDir());
        } catch (IOException ex) {
            throw new IOException("cannot create workdir: " + config.workDir(), ex);
        }
        if (!Files.isDirectory(config.workDir())) {
            throw new IOException("workdir path is not a directory: " + config.workDir());
        }
    }
}
