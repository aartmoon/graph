package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.pagerank.PageRankEngine.PageRankRunResult;
import org.example.largegraph.storage.DiskDoubleArray;
import org.example.largegraph.storage.DiskIntArray;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public final class PageRankResultWriter {
    private final AppConfig config;

    public PageRankResultWriter(AppConfig config) {
        this.config = config;
    }

    public void write(PageRankRunResult result) throws IOException {
        if (config.output().getParent() != null) {
            Files.createDirectories(config.output().getParent());
        }
        if (config.topK() > 0) {
            writeTopK(result);
        } else {
            writeAllStreaming(result);
        }
    }

    private void writeAllStreaming(PageRankRunResult result) throws IOException {
        try (DiskDoubleArray ranks = new DiskDoubleArray(result.rankPath(), result.vertexCount(), config.chunkSize(), false);
             DiskIntArray originalIds = new DiskIntArray(result.verticesPath(), result.vertexCount(), config.chunkSize(), false);
             BufferedWriter writer = Files.newBufferedWriter(config.output(), StandardCharsets.UTF_8)) {
            writer.write("vertex,rank");
            writer.newLine();
            for (long start = 0; start < result.vertexCount(); start += config.chunkSize()) {
                int length = (int) Math.min(config.chunkSize(), result.vertexCount() - start);
                double[] rankChunk = ranks.readChunk(start, length);
                int[] originalIdChunk = originalIds.readIntChunk(start, length);
                for (int i = 0; i < length; i++) {
                    writer.write(Integer.toString(originalIdChunk[i]));
                    writer.write(',');
                    writer.write(Double.toString(rankChunk[i]));
                    writer.newLine();
                }
            }
        }
    }

    private void writeTopK(PageRankRunResult result) throws IOException {
        Comparator<RankEntry> minRankComparator = Comparator
                .comparingDouble(RankEntry::rank)
                .thenComparing(Comparator.comparingInt(RankEntry::vertex).reversed());
        PriorityQueue<RankEntry> heap = new PriorityQueue<>(minRankComparator);

        try (DiskDoubleArray ranks = new DiskDoubleArray(result.rankPath(), result.vertexCount(), config.chunkSize(), false);
             DiskIntArray originalIds = new DiskIntArray(result.verticesPath(), result.vertexCount(), config.chunkSize(), false)) {
            for (long start = 0; start < result.vertexCount(); start += config.chunkSize()) {
                int length = (int) Math.min(config.chunkSize(), result.vertexCount() - start);
                double[] rankChunk = ranks.readChunk(start, length);
                int[] originalIdChunk = originalIds.readIntChunk(start, length);
                for (int i = 0; i < length; i++) {
                    offerTopK(heap, originalIdChunk[i], rankChunk[i]);
                }
            }
        }

        List<RankEntry> entries = new ArrayList<>(heap);
        entries.sort(Comparator
                .comparingDouble(RankEntry::rank)
                .reversed()
                .thenComparingInt(RankEntry::vertex));

        try (BufferedWriter writer = Files.newBufferedWriter(config.output(), StandardCharsets.UTF_8)) {
            writer.write("vertex,rank");
            writer.newLine();
            for (RankEntry entry : entries) {
                writer.write(Integer.toString(entry.vertex()));
                writer.write(',');
                writer.write(Double.toString(entry.rank()));
                writer.newLine();
            }
        }
    }

    private void offerTopK(PriorityQueue<RankEntry> heap, int vertex, double rank) {
        if (heap.size() < config.topK()) {
            heap.add(new RankEntry(vertex, rank));
            return;
        }

        RankEntry weakest = heap.peek();
        if (weakest == null) {
            return;
        }
        if (rank > weakest.rank()
                || (rank == weakest.rank() && vertex < weakest.vertex())) {
            heap.poll();
            heap.add(new RankEntry(vertex, rank));
        }
    }

    private record RankEntry(int vertex, double rank) {
    }
}
