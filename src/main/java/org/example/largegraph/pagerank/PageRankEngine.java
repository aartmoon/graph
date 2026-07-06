package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.graph.GraphPreprocessor.PreprocessingResult;
import org.example.largegraph.io.BinaryEdgeReader;
import org.example.largegraph.io.BinaryEdgeReader.DenseEdge;
import org.example.largegraph.io.BinaryMessageReader;
import org.example.largegraph.io.BinaryMessageReader.Message;
import org.example.largegraph.io.MessagePartitionWriterManager;
import org.example.largegraph.storage.DiskDoubleArray;
import org.example.largegraph.storage.DiskIntArray;
import org.example.largegraph.util.MemoryUtils;
import org.example.largegraph.util.ProgressLogger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class PageRankEngine {
    private static final double RANK_SUM_WARNING_THRESHOLD = 1e-6;

    private final AppConfig config;
    private final ProgressLogger logger;

    public PageRankEngine(AppConfig config, ProgressLogger logger) {
        this.config = config;
        this.logger = logger;
    }

    public PageRankRunResult run(PreprocessingResult graph) throws IOException {
        int workerCount = Math.max(1, Math.min(config.threads(), graph.sourcePartitionCount()));
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        double lastDiff = Double.POSITIVE_INFINITY;
        double lastRankSum = Double.NaN;
        int completedIterations = 0;

        try {
            for (int iteration = 1; iteration <= config.maxIterations(); iteration++) {
                long startedAt = System.nanoTime();
                Path iterationMessagesDir = messagesDir(iteration);
                deleteRecursively(iterationMessagesDir);
                Files.createDirectories(iterationMessagesDir);

                double danglingMass = calculateDanglingMass(graph);
                scatter(graph, iterationMessagesDir, executor);
                double base = (1.0 - config.damping()) / graph.vertexCount()
                        + config.damping() * danglingMass / graph.vertexCount();
                gather(graph, iterationMessagesDir, base, executor);

                ConvergenceStats stats = calculateConvergence(graph);
                lastDiff = stats.l1Diff();
                lastRankSum = stats.rankSum();
                if (Math.abs(lastRankSum - 1.0) > RANK_SUM_WARNING_THRESHOLD) {
                    logger.info("WARNING rankSum drift: %.12g".formatted(lastRankSum));
                }

                swapRankFiles(graph.rankCurrentPath(), graph.rankNextPath());
                if (!config.keepMessages()) {
                    deleteRecursively(iterationMessagesDir);
                }
                completedIterations = iteration;
                logIteration(iteration, lastDiff, lastRankSum, startedAt);

                if (lastDiff < config.epsilon()) {
                    return new PageRankRunResult(
                            graph.rankCurrentPath(),
                            graph.verticesPath(),
                            graph.vertexCount(),
                            completedIterations,
                            lastDiff,
                            lastRankSum,
                            "CONVERGED"
                    );
                }
            }

            return new PageRankRunResult(
                    graph.rankCurrentPath(),
                    graph.verticesPath(),
                    graph.vertexCount(),
                    completedIterations,
                    lastDiff,
                    lastRankSum,
                    "MAX_ITERATIONS_REACHED"
            );
        } finally {
            shutdown(executor);
        }
    }

    private double calculateDanglingMass(PreprocessingResult graph) throws IOException {
        double danglingMass = 0.0;
        try (DiskDoubleArray ranks = new DiskDoubleArray(graph.rankCurrentPath(), graph.vertexCount(), graph.chunkSize(), false);
             DiskIntArray outDegree = new DiskIntArray(graph.outDegreePath(), graph.vertexCount(), graph.chunkSize(), false)) {
            for (long start = 0; start < graph.vertexCount(); start += graph.chunkSize()) {
                int length = chunkLength(graph, start);
                double[] rankChunk = ranks.readChunk(start, length);
                int[] degreeChunk = outDegree.readIntChunk(start, length);
                for (int i = 0; i < length; i++) {
                    if (degreeChunk[i] == 0) {
                        danglingMass += rankChunk[i];
                    }
                }
            }
        }
        return danglingMass;
    }

    private void scatter(
            PreprocessingResult graph,
            Path iterationMessagesDir,
            ExecutorService executor
    ) throws IOException {
        List<SourcePartitionWork> workerBuckets = balancedSourcePartitionBuckets(graph);
        List<Callable<ScatterStats>> tasks = new ArrayList<>(workerBuckets.size());
        for (SourcePartitionWork work : workerBuckets) {
            tasks.add(() -> scatterSourcePartitions(graph, iterationMessagesDir, work));
        }
        List<ScatterStats> stats = invokeAll(executor, tasks);
        long messages = 0;
        long bytes = 0;
        for (ScatterStats stat : stats) {
            messages += stat.messageCount();
            bytes += stat.messageBytes();
        }
        logger.info("scatter messages=%d bytes=%d tasks=%d".formatted(messages, bytes, tasks.size()));
    }

    private ScatterStats scatterSourcePartitions(
            PreprocessingResult graph,
            Path iterationMessagesDir,
            SourcePartitionWork work
    ) throws IOException {
        long edgeCount = 0;
        long messageCount = 0;
        Path workerDir = iterationMessagesDir.resolve("worker-%05d".formatted(work.workerId()));
        int maxOpenMessageWriters = Math.min(64, Math.max(4, config.threads() * 4));

        try (DiskDoubleArray ranks = new DiskDoubleArray(graph.rankCurrentPath(), graph.vertexCount(), graph.chunkSize(), false);
             DiskIntArray outDegree = new DiskIntArray(graph.outDegreePath(), graph.vertexCount(), graph.chunkSize(), false);
             MessagePartitionWriterManager messageWriters =
                     new MessagePartitionWriterManager(workerDir, graph.destinationPartitionCount(), maxOpenMessageWriters)) {
            for (int sourcePartition : work.partitions()) {
                long sourceStart = (long) sourcePartition * graph.chunkSize();
                int sourceLength = chunkLength(graph, sourceStart);
                double[] rankChunk = ranks.readChunk(sourceStart, sourceLength);
                int[] outDegreeChunk = outDegree.readIntChunk(sourceStart, sourceLength);
                Path sourcePartitionPath = graph.edgesDir()
                        .resolve("src-part-%05d.bin".formatted(sourcePartition));
                if (!Files.exists(sourcePartitionPath)) {
                    continue;
                }

                try (BinaryEdgeReader edgeReader = new BinaryEdgeReader(sourcePartitionPath)) {
                    Optional<DenseEdge> edge;
                    while ((edge = edgeReader.next()).isPresent()) {
                        int from = edge.get().denseFrom();
                        int to = edge.get().denseTo();
                        int localFrom = Math.toIntExact(from - sourceStart);
                        int degree = outDegreeChunk[localFrom];
                        if (degree > 0) {
                            double contribution = config.damping() * rankChunk[localFrom] / degree;
                            int destinationPartition = to / graph.chunkSize();
                            messageWriters.write(destinationPartition, to, contribution);
                            messageCount++;
                        }
                        edgeCount++;
                    }
                }
            }
        }
        long messageBytes = messageBytesFromManifest(workerDir);
        return new ScatterStats(edgeCount, messageCount, messageBytes);
    }

    private void gather(
            PreprocessingResult graph,
            Path iterationMessagesDir,
            double base,
            ExecutorService executor
    ) throws IOException {
        Map<Integer, List<MessageFile>> messageIndex = buildDestinationMessageIndex(iterationMessagesDir);
        try (DiskDoubleArray nextRanks = new DiskDoubleArray(graph.rankNextPath(), graph.vertexCount(), graph.chunkSize())) {
            List<Callable<GatherStats>> tasks = new ArrayList<>(graph.destinationPartitionCount());
            for (int destinationPartition : destinationPartitionsByDescendingMessageBytes(graph, messageIndex)) {
                List<MessageFile> messageFiles = messageIndex.getOrDefault(destinationPartition, List.of());
                tasks.add(() -> gatherPartition(graph, base, nextRanks, destinationPartition, messageFiles));
            }
            List<GatherStats> stats = invokeAll(executor, tasks);
            nextRanks.flush();
            long messages = 0;
            for (GatherStats stat : stats) {
                messages += stat.messageCount();
            }
            logger.info("gather messages=%d tasks=%d".formatted(messages, tasks.size()));
        }
    }

    private GatherStats gatherPartition(
            PreprocessingResult graph,
            double base,
            DiskDoubleArray nextRanks,
            int destinationPartition,
            List<MessageFile> messageFiles
    ) throws IOException {
        long destinationStart = (long) destinationPartition * graph.chunkSize();
        int destinationLength = chunkLength(graph, destinationStart);
        double[] nextChunk = new double[destinationLength];
        long messageCount = 0;

        for (MessageFile messageFile : messageFiles) {
            try (BinaryMessageReader reader = new BinaryMessageReader(messageFile.path())) {
                Optional<Message> message;
                while ((message = reader.next()).isPresent()) {
                    int localTo = Math.toIntExact(message.get().to() - destinationStart);
                    nextChunk[localTo] += message.get().contribution();
                    messageCount++;
                }
            }
        }

        for (int i = 0; i < destinationLength; i++) {
            nextChunk[i] += base;
        }
        nextRanks.writeChunk(destinationStart, nextChunk, destinationLength);
        return new GatherStats(messageCount);
    }

    private List<SourcePartitionWork> balancedSourcePartitionBuckets(PreprocessingResult graph) throws IOException {
        int workerCount = Math.max(1, Math.min(config.threads(), graph.sourcePartitionCount()));
        List<SourcePartitionWorkBuilder> buckets = new ArrayList<>(workerCount);
        for (int workerId = 0; workerId < workerCount; workerId++) {
            buckets.add(new SourcePartitionWorkBuilder(workerId));
        }

        List<Integer> partitions = new ArrayList<>(graph.sourcePartitionCount());
        for (int partition = 0; partition < graph.sourcePartitionCount(); partition++) {
            if (sourcePartitionSize(graph, partition) > 0) {
                partitions.add(partition);
            }
        }
        partitions.sort(Comparator
                .comparingLong((Integer partition) -> sourcePartitionSize(graph, partition))
                .reversed()
                .thenComparingInt(Integer::intValue));

        for (int partition : partitions) {
            SourcePartitionWorkBuilder lightest = buckets.get(0);
            for (SourcePartitionWorkBuilder bucket : buckets) {
                if (bucket.bytes() < lightest.bytes()) {
                    lightest = bucket;
                }
            }
            lightest.add(partition, sourcePartitionSize(graph, partition));
        }

        List<SourcePartitionWork> work = new ArrayList<>(workerCount);
        for (SourcePartitionWorkBuilder bucket : buckets) {
            if (!bucket.partitions().isEmpty()) {
                work.add(bucket.build());
            }
        }
        return work;
    }

    private long sourcePartitionSize(PreprocessingResult graph, int partition) {
        try {
            Path path = graph.edgesDir().resolve("src-part-%05d.bin".formatted(partition));
            return Files.exists(path) ? Files.size(path) : 0L;
        } catch (IOException ex) {
            throw new IllegalStateException("cannot read source partition size: " + partition, ex);
        }
    }

    private List<Integer> destinationPartitionsByDescendingMessageBytes(
            PreprocessingResult graph,
            Map<Integer, List<MessageFile>> messageIndex
    ) {
        List<Integer> partitions = new ArrayList<>(graph.destinationPartitionCount());
        for (int partition = 0; partition < graph.destinationPartitionCount(); partition++) {
            partitions.add(partition);
        }
        partitions.sort(Comparator
                .comparingLong((Integer partition) -> messageIndex
                        .getOrDefault(partition, List.of())
                        .stream()
                        .mapToLong(MessageFile::bytes)
                        .sum())
                .reversed()
                .thenComparingInt(Integer::intValue));
        return partitions;
    }

    private Map<Integer, List<MessageFile>> buildDestinationMessageIndex(Path iterationMessagesDir) throws IOException {
        Map<Integer, List<MessageFile>> index = new HashMap<>();
        if (!Files.exists(iterationMessagesDir)) {
            return index;
        }
        try (var paths = Files.list(iterationMessagesDir)) {
            for (Path workerDir : paths.filter(Files::isDirectory).sorted().toList()) {
                Path manifest = workerDir.resolve("manifest.txt");
                if (!Files.exists(manifest)) {
                    continue;
                }
                for (String line : Files.readAllLines(manifest, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    int partition = Integer.parseInt(line.trim());
                    Path messagePath = workerDir.resolve("msg-part-%05d.bin".formatted(partition));
                    long bytes = Files.size(messagePath);
                    index.computeIfAbsent(partition, ignored -> new ArrayList<>())
                            .add(new MessageFile(messagePath, bytes));
                }
            }
        }
        return index;
    }

    private long messageBytesFromManifest(Path workerDir) throws IOException {
        long bytes = 0;
        Path manifest = workerDir.resolve("manifest.txt");
        if (!Files.exists(manifest)) {
            return 0;
        }
        for (String line : Files.readAllLines(manifest, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            int partition = Integer.parseInt(line.trim());
            bytes += Files.size(workerDir.resolve("msg-part-%05d.bin".formatted(partition)));
        }
        return bytes;
    }

    private ConvergenceStats calculateConvergence(PreprocessingResult graph) throws IOException {
        double l1Diff = 0.0;
        double rankSum = 0.0;
        try (DiskDoubleArray current = new DiskDoubleArray(graph.rankCurrentPath(), graph.vertexCount(), graph.chunkSize(), false);
             DiskDoubleArray next = new DiskDoubleArray(graph.rankNextPath(), graph.vertexCount(), graph.chunkSize(), false)) {
            for (long start = 0; start < graph.vertexCount(); start += graph.chunkSize()) {
                int length = chunkLength(graph, start);
                double[] currentChunk = current.readChunk(start, length);
                double[] nextChunk = next.readChunk(start, length);
                for (int i = 0; i < length; i++) {
                    l1Diff += Math.abs(nextChunk[i] - currentChunk[i]);
                    rankSum += nextChunk[i];
                }
            }
        }
        return new ConvergenceStats(l1Diff, rankSum);
    }

    private void swapRankFiles(Path currentPath, Path nextPath) throws IOException {
        Path tmpPath = currentPath.resolveSibling("rank_swap.tmp");
        Files.move(currentPath, tmpPath, StandardCopyOption.REPLACE_EXISTING);
        Files.move(nextPath, currentPath, StandardCopyOption.REPLACE_EXISTING);
        Files.move(tmpPath, nextPath, StandardCopyOption.REPLACE_EXISTING);
    }

    private int chunkLength(PreprocessingResult graph, long start) {
        return (int) Math.min(graph.chunkSize(), graph.vertexCount() - start);
    }

    private Path messagesDir(int iteration) {
        return config.workDir()
                .resolve("messages")
                .resolve("iter-%06d".formatted(iteration));
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

    private void logIteration(int iteration, double diff, double rankSum, long startedAt) {
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);
        logger.info("iteration=%d diff=%.12g rankSum=%.12g usedHeap=%s elapsedMs=%d".formatted(
                iteration,
                diff,
                rankSum,
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

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var paths = Files.walk(path)) {
            for (Path child : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(child);
            }
        }
    }

    public record PageRankRunResult(
            Path rankPath,
            Path verticesPath,
            long vertexCount,
            int iterations,
            double lastDelta,
            double rankSum,
            String status
    ) {
    }

    private record ScatterStats(long edgeCount, long messageCount, long messageBytes) {
    }

    private record GatherStats(long messageCount) {
    }

    private record MessageFile(Path path, long bytes) {
    }

    private record ConvergenceStats(double l1Diff, double rankSum) {
    }

    private record SourcePartitionWork(int workerId, List<Integer> partitions, long bytes) {
    }

    private static final class SourcePartitionWorkBuilder {
        private final int workerId;
        private final List<Integer> partitions = new ArrayList<>();
        private long bytes;

        private SourcePartitionWorkBuilder(int workerId) {
            this.workerId = workerId;
        }

        private void add(int partition, long partitionBytes) {
            partitions.add(partition);
            bytes += partitionBytes;
        }

        private int workerId() {
            return workerId;
        }

        private List<Integer> partitions() {
            return partitions;
        }

        private long bytes() {
            return bytes;
        }

        private SourcePartitionWork build() {
            return new SourcePartitionWork(workerId, List.copyOf(partitions), bytes);
        }
    }
}
