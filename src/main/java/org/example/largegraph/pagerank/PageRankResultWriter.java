package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.graph.VertexIndexer;
import org.example.largegraph.pagerank.PageRankEngine.PageRankRunResult;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;

public final class PageRankResultWriter {
    private final AppConfig config;

    public PageRankResultWriter(AppConfig config) {
        this.config = config;
    }

    public void write(PageRankRunResult result, VertexIndexer indexer) throws IOException {
        Integer[] denseIds = IntStream.range(0, result.ranks().length).boxed().toArray(Integer[]::new);
        if (config.sortByRank()) {
            Arrays.sort(denseIds, Comparator
                    .comparingDouble((Integer id) -> result.ranks()[id])
                    .reversed()
                    .thenComparingInt(indexer::originalId));
        } else {
            Arrays.sort(denseIds, Comparator.comparingInt(indexer::originalId));
        }

        try (BufferedWriter writer = Files.newBufferedWriter(config.output(), StandardCharsets.UTF_8)) {
            writer.write("vertex,rank");
            writer.newLine();
            for (int denseId : denseIds) {
                writer.write(Integer.toString(indexer.originalId(denseId)));
                writer.write(',');
                writer.write(Double.toString(result.ranks()[denseId]));
                writer.newLine();
            }
        }
    }
}
