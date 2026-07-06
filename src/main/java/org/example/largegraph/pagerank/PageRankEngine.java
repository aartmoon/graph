package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.graph.GraphPreprocessor.PreprocessingResult;
import org.example.largegraph.graph.OutDegreeStore;
import org.example.largegraph.graph.RankStore;
import org.example.largegraph.io.BinaryEdgeReader;
import org.example.largegraph.io.BinaryEdgeReader.DenseEdge;
import org.example.largegraph.util.MemoryUtils;
import org.example.largegraph.util.ProgressLogger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class PageRankEngine {
    private final AppConfig config;
    private final ProgressLogger logger;

    public PageRankEngine(AppConfig config, ProgressLogger logger) {
        this.config = config;
        this.logger = logger;
    }

    public PageRankRunResult run(PreprocessingResult graph) throws IOException {
        int vertexCount = graph.vertexCount();
        if (vertexCount == 0) {
            throw new IllegalArgumentException("cannot run PageRank on an empty graph");
        }

        RankStore rankStore = new RankStore(config.workDir());
        int[] outDegree = new OutDegreeStore(config.workDir()).read(vertexCount);
        double[] currentRank = rankStore.readCurrent(vertexCount);
        double[] nextRank = new double[vertexCount];

        int threadCount = Math.max(1, config.threads());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            return runIterations(graph, rankStore, outDegree, currentRank, nextRank, executor);
        } finally {
            shutdown(executor);
        }
    }

    private PageRankRunResult runIterations(
            PreprocessingResult graph,
            RankStore rankStore,
            int[] outDegree,
            double[] currentRank,
            double[] nextRank,
            ExecutorService executor
    ) throws IOException {
        double lastDiff = Double.POSITIVE_INFINITY;
        int completedIterations = 0;

        for (int iteration = 1; iteration <= config.maxIterations(); iteration++) {
            long startedAt = System.nanoTime();
            Arrays.fill(nextRank, 0.0);

            double danglingMass = calculateDanglingMass(currentRank, outDegree, executor);
            long processedEdges = processPartitions(graph.partitionCount(), currentRank, nextRank, outDegree, executor);
            if (processedEdges != graph.edgeCount()) {
                throw new IOException("processed edge count mismatch: expected=%d actual=%d"
                        .formatted(graph.edgeCount(), processedEdges));
            }
            lastDiff = applyBaseAndCalculateDiff(currentRank, nextRank, danglingMass);

            rankStore.writeNext(nextRank);
            double[] previousRank = currentRank;
            currentRank = nextRank;
            nextRank = previousRank;
            rankStore.writeCurrent(currentRank);

            completedIterations = iteration;
            logIteration(iteration, lastDiff, startedAt);

            if (lastDiff < config.epsilon()) {
                return new PageRankRunResult(currentRank, completedIterations, lastDiff, "CONVERGED");
            }
        }

        return new PageRankRunResult(currentRank, completedIterations, lastDiff, "MAX_ITERATIONS_REACHED");
    }

    private double calculateDanglingMass(double[] currentRank, int[] outDegree, ExecutorService executor) throws IOException {
        int taskCount = Math.min(config.threads(), currentRank.length);
        int rangeSize = (int) Math.ceil(currentRank.length / (double) taskCount);
        List<Callable<Double>> tasks = new ArrayList<>(taskCount);

        for (int start = 0; start < currentRank.length; start += rangeSize) {
            int fromInclusive = start;
            int toExclusive = Math.min(currentRank.length, start + rangeSize);
            tasks.add(() -> {
                double sum = 0.0;
                for (int vertex = fromInclusive; vertex < toExclusive; vertex++) {
                    if (outDegree[vertex] == 0) {
                        sum += currentRank[vertex];
                    }
                }
                return sum;
            });
        }

        double danglingMass = 0.0;
        for (double partial : invokeAll(executor, tasks)) {
            danglingMass += partial;
        }
        return danglingMass;
    }

    private long processPartitions(
            int partitionCount,
            double[] currentRank,
            double[] nextRank,
            int[] outDegree,
            ExecutorService executor
    ) throws IOException {
        List<Callable<PartitionStats>> tasks = new ArrayList<>(partitionCount);
        for (int partition = 0; partition < partitionCount; partition++) {
            int partitionId = partition;
            tasks.add(() -> processPartition(partitionId, currentRank, nextRank, outDegree));
        }
        long edgeCount = 0;
        for (PartitionStats stats : invokeAll(executor, tasks)) {
            edgeCount += stats.edgeCount();
        }
        return edgeCount;
    }

    private PartitionStats processPartition(
            int partitionId,
            double[] currentRank,
            double[] nextRank,
            int[] outDegree
    ) throws IOException {
        Path partitionPath = config.workDir()
                .resolve("partitions")
                .resolve("part-%05d.bin".formatted(partitionId));
        long edgeCount = 0;

        try (BinaryEdgeReader reader = new BinaryEdgeReader(partitionPath)) {
            Optional<DenseEdge> edge;
            while ((edge = reader.next()).isPresent()) {
                int denseFrom = edge.get().denseFrom();
                int denseTo = edge.get().denseTo();
                int degree = outDegree[denseFrom];
                if (degree > 0) {
                    nextRank[denseTo] += config.damping() * currentRank[denseFrom] / degree;
                }
                edgeCount++;
            }
        }

        return new PartitionStats(edgeCount);
    }

    private double applyBaseAndCalculateDiff(double[] currentRank, double[] nextRank, double danglingMass) {
        double base = (1.0 - config.damping()) / currentRank.length
                + config.damping() * danglingMass / currentRank.length;
        double diff = 0.0;

        for (int vertex = 0; vertex < nextRank.length; vertex++) {
            nextRank[vertex] += base;
            diff += Math.abs(nextRank[vertex] - currentRank[vertex]);
        }

        return diff;
    }

    private <T> List<T> invokeAll(ExecutorService executor, List<Callable<T>> tasks) throws IOException {
        try {
            List<Future<T>> futures = executor.invokeAll(tasks);
            List<T> results = new ArrayList<>(futures.size());
            for (Future<T> future : futures) {
                results.add(future.get());
            }
            return results;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("PageRank execution was interrupted", ex);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("PageRank task failed", cause);
        }
    }

    private void logIteration(int iteration, double diff, long startedAt) {
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        logger.info("iteration=%d diff=%.12g usedHeap=%s elapsedMs=%d".formatted(
                iteration,
                diff,
                MemoryUtils.humanReadableBytes(MemoryUtils.usedHeapBytes()),
                elapsedMillis
        ));
    }

    private void shutdown(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    public record PageRankRunResult(
            double[] ranks,
            int iterations,
            double lastDelta,
            String status
    ) {
    }

    private record PartitionStats(long edgeCount) {
    }
}
