package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.graph.GraphPreprocessor.PreprocessingResult;
import org.example.largegraph.io.BinaryMessageReader;
import org.example.largegraph.io.MessageBucketLayout;
import org.example.largegraph.io.MessagePartitionWriterManager;
import org.example.largegraph.storage.DiskDoubleArray;
import org.example.largegraph.storage.DiskIntArray;
import org.example.largegraph.util.FileUtils;
import org.example.largegraph.util.MemoryUtils;
import org.example.largegraph.util.ProgressLogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class PageRankEngine {
    private static final double RANK_SUM_WARNING_THRESHOLD = 1e-6;
    private static final int EDGE_RECORD_BYTES = Integer.BYTES * 2;
    private static final int EDGE_READ_BUFFER_BYTES = 64 * 1024;
    private static final int MAX_TOTAL_MESSAGE_WRITERS = 256;
    private static final long HEAVY_GATHER_BUCKET_BYTES = 64L * 1024L * 1024L;

    private final AppConfig config;
    private final ProgressLogger logger;

    public PageRankEngine(AppConfig config, ProgressLogger logger) {
        this.config = config;
        this.logger = logger;
    }

    public PageRankRunResult run(PreprocessingResult graph) throws IOException {
        List<SourcePartitionSlice> sourceSlices = readNonEmptySourcePartitionSlices(graph);
        List<SourcePartitionWork> scatterWork = balancedSourcePartitionBuckets(sourceSlices);
        ExecutorService executor = Executors.newFixedThreadPool(config.threads());
        double lastDiff = Double.POSITIVE_INFINITY;
        double lastRankSum = Double.NaN;
        int completedIterations = 0;

        try {
            for (int iteration = 1; iteration <= config.maxIterations(); iteration++) {
                long startedAt = System.nanoTime();
                Path iterationMessagesDir = messagesDir(iteration);
                deleteRecursively(iterationMessagesDir);
                Files.createDirectories(iterationMessagesDir);

                double danglingMass = calculateDanglingMass(graph, executor);
                scatter(graph, iterationMessagesDir, executor, scatterWork);
                double base = (1.0 - config.damping()) / graph.vertexCount()
                        + config.damping() * danglingMass / graph.vertexCount();
                gather(graph, iterationMessagesDir, base, executor);

                ConvergenceStats stats = calculateConvergence(graph, executor);
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
                            graph.edgeCount(),
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
                    graph.edgeCount(),
                    completedIterations,
                    lastDiff,
                    lastRankSum,
                    "MAX_ITERATIONS_REACHED"
            );
        } finally {
            shutdown(executor);
        }
    }

    private double calculateDanglingMass(PreprocessingResult graph, ExecutorService executor) throws IOException {
        List<ChunkRange> ranges = chunkRanges(graph);
        List<Callable<Double>> tasks = new ArrayList<>(ranges.size());
        for (ChunkRange range : ranges) {
            tasks.add(() -> calculateDanglingMassRange(graph, range));
        }
        double danglingMass = 0.0;
        for (double localMass : invokeAll(executor, tasks)) {
            danglingMass += localMass;
        }
        return danglingMass;
    }

    private double calculateDanglingMassRange(PreprocessingResult graph, ChunkRange range) throws IOException {
        double danglingMass = 0.0;
        try (DiskDoubleArray ranks = new DiskDoubleArray(graph.rankCurrentPath(), graph.vertexCount(), graph.chunkSize(), false);
             DiskIntArray outDegree = new DiskIntArray(graph.outDegreePath(), graph.vertexCount(), graph.chunkSize(), false)) {
            for (long start = range.startId(); start < range.endIdExclusive(); start += graph.chunkSize()) {
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
            ExecutorService executor,
            List<SourcePartitionWork> workerBuckets
    ) throws IOException {
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
        int maxOpenMessageWriters = maxOpenMessageWritersPerWorker();

        try (DiskDoubleArray ranks = new DiskDoubleArray(graph.rankCurrentPath(), graph.vertexCount(), graph.chunkSize(), false);
             DiskIntArray outDegree = new DiskIntArray(graph.outDegreePath(), graph.vertexCount(), graph.chunkSize(), false);
             MessagePartitionWriterManager messageWriters =
                     new MessagePartitionWriterManager(workerDir, graph.destinationPartitionCount(), maxOpenMessageWriters)) {
            Map<Integer, List<SourcePartitionSlice>> slicesByPartition = slicesBySourcePartition(work.slices());
            for (Map.Entry<Integer, List<SourcePartitionSlice>> entry : slicesByPartition.entrySet()) {
                int sourcePartition = entry.getKey();
                long sourceStart = (long) sourcePartition * graph.chunkSize();
                int sourceLength = chunkLength(graph, sourceStart);
                double[] rankChunk = ranks.readChunk(sourceStart, sourceLength);
                int[] outDegreeChunk = outDegree.readIntChunk(sourceStart, sourceLength);
                Path sourcePartitionPath = graph.edgesDir()
                        .resolve("src-part-%05d.bin".formatted(sourcePartition));
                if (!Files.exists(sourcePartitionPath)) {
                    continue;
                }

                try (FileChannel channel = FileChannel.open(sourcePartitionPath, StandardOpenOption.READ)) {
                    ByteBuffer buffer = ByteBuffer.allocate(EDGE_READ_BUFFER_BYTES);
                    for (SourcePartitionSlice slice : entry.getValue()) {
                        ScatterStats sliceStats = scatterSourcePartitionSlice(
                                graph,
                                messageWriters,
                                channel,
                                slice,
                                sourceStart,
                                rankChunk,
                                outDegreeChunk,
                                buffer
                        );
                        edgeCount += sliceStats.edgeCount();
                        messageCount += sliceStats.messageCount();
                    }
                }
            }
        }
        long messageBytes = messageBytesFromManifest(workerDir);
        return new ScatterStats(edgeCount, messageCount, messageBytes);
    }

    private int maxOpenMessageWritersPerWorker() {
        int totalBudget = Math.max(config.threads(), Math.min(MAX_TOTAL_MESSAGE_WRITERS, config.threads() * 8));
        return Math.max(1, totalBudget / config.threads());
    }

    private Map<Integer, List<SourcePartitionSlice>> slicesBySourcePartition(List<SourcePartitionSlice> slices) {
        Map<Integer, List<SourcePartitionSlice>> grouped = new LinkedHashMap<>();
        for (SourcePartitionSlice slice : slices) {
            grouped.computeIfAbsent(slice.partition(), ignored -> new ArrayList<>()).add(slice);
        }
        return grouped;
    }

    private ScatterStats scatterSourcePartitionSlice(
            PreprocessingResult graph,
            MessagePartitionWriterManager messageWriters,
            FileChannel channel,
            SourcePartitionSlice slice,
            long sourceStart,
            double[] rankChunk,
            int[] outDegreeChunk,
            ByteBuffer buffer
    ) throws IOException {
        long edgeCount = 0;
        long messageCount = 0;

        long position = slice.startByte();
        long end = slice.startByte() + slice.bytes();
        while (position < end) {
            int bytesToRead = Math.toIntExact(Math.min(buffer.capacity(), end - position));
            buffer.clear();
            buffer.limit(bytesToRead);
            readFully(channel, buffer, position);
            position += bytesToRead;
            buffer.flip();

            while (buffer.hasRemaining()) {
                int from = buffer.getInt();
                int to = buffer.getInt();
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
        return new ScatterStats(edgeCount, messageCount, 0);
    }

    private void gather(
            PreprocessingResult graph,
            Path iterationMessagesDir,
            double base,
            ExecutorService executor
    ) throws IOException {
        Map<Integer, List<MessageFile>> messageIndex = buildMessageBucketIndex(iterationMessagesDir);
        MessageBucketLayout bucketLayout = new MessageBucketLayout(graph.destinationPartitionCount());
        validateMessageBuckets(messageIndex, bucketLayout);

        List<Callable<GatherStats>> tasks = new ArrayList<>(bucketLayout.bucketCount());
        for (int bucket : bucketsByDescendingMessageBytes(bucketLayout, messageIndex)) {
            List<MessageFile> messageFiles = messageIndex.getOrDefault(bucket, List.of());
            addGatherTasks(tasks, graph, bucketLayout, bucket, messageFiles, base);
        }
        List<GatherStats> stats = invokeAll(executor, tasks);
        long messages = 0;
        for (GatherStats stat : stats) {
            messages += stat.messageCount();
        }
        logger.info("gather messages=%d tasks=%d".formatted(messages, tasks.size()));
    }

    private void addGatherTasks(
            List<Callable<GatherStats>> tasks,
            PreprocessingResult graph,
            MessageBucketLayout bucketLayout,
            int bucket,
            List<MessageFile> messageFiles,
            double base
    ) {
        int firstPartition = bucketLayout.firstPartition(bucket);
        int endPartitionExclusive = bucketLayout.endPartitionExclusive(bucket);
        long bytes = messageFiles.stream().mapToLong(MessageFile::bytes).sum();
        int partitionCount = endPartitionExclusive - firstPartition;
        int splitCount = gatherSplitCount(bytes, partitionCount);
        if (splitCount <= 1) {
            tasks.add(() -> gatherBucketRange(
                    graph,
                    bucket,
                    firstPartition,
                    endPartitionExclusive,
                    firstPartition,
                    endPartitionExclusive,
                    messageFiles,
                    base
            ));
            return;
        }

        int partitionsPerSplit = Math.ceilDiv(partitionCount, splitCount);
        for (int split = 0; split < splitCount; split++) {
            int splitStart = firstPartition + split * partitionsPerSplit;
            int splitEnd = Math.min(endPartitionExclusive, splitStart + partitionsPerSplit);
            if (splitStart < splitEnd) {
                tasks.add(() -> gatherBucketRange(
                        graph,
                        bucket,
                        firstPartition,
                        endPartitionExclusive,
                        splitStart,
                        splitEnd,
                        messageFiles,
                        base
                ));
            }
        }
    }

    private int gatherSplitCount(long bytes, int partitionCount) {
        if (bytes <= HEAVY_GATHER_BUCKET_BYTES || partitionCount <= 1) {
            return 1;
        }
        long bySize = Math.ceilDiv(bytes, HEAVY_GATHER_BUCKET_BYTES);
        return (int) Math.min(Math.min(bySize, config.threads()), partitionCount);
    }

    private GatherStats gatherBucketRange(
            PreprocessingResult graph,
            int bucket,
            int bucketFirstPartition,
            int bucketEndPartitionExclusive,
            int rangeFirstPartition,
            int rangeEndPartitionExclusive,
            List<MessageFile> messageFiles,
            double base
    ) throws IOException {
        long messageCount = 0;

        try (DiskDoubleArray nextRanks = new DiskDoubleArray(graph.rankNextPath(), graph.vertexCount(), graph.chunkSize(), true, false)) {
            GatherChunkCache chunkCache = new GatherChunkCache(graph, nextRanks, base, config.gatherChunkCacheSize());
            for (MessageFile messageFile : messageFiles) {
                try (BinaryMessageReader reader = new BinaryMessageReader(messageFile.path())) {
                    while (reader.next()) {
                        int to = reader.to();
                        int destinationPartition = to / graph.chunkSize();
                        if (destinationPartition < bucketFirstPartition || destinationPartition >= bucketEndPartitionExclusive) {
                            throw new IOException("message destination partition does not belong to bucket: bucket=%d partition=%d"
                                    .formatted(bucket, destinationPartition));
                        }
                        if (destinationPartition < rangeFirstPartition || destinationPartition >= rangeEndPartitionExclusive) {
                            continue;
                        }
                        CachedGatherChunk chunk = chunkCache.chunk(destinationPartition);
                        int localTo = Math.toIntExact(to - chunk.start());
                        chunk.values()[localTo] += reader.contribution();
                        messageCount++;
                    }
                }
            }
            chunkCache.flushRange(rangeFirstPartition, rangeEndPartitionExclusive);
        }
        return new GatherStats(messageCount);
    }

    private List<SourcePartitionWork> balancedSourcePartitionBuckets(List<SourcePartitionSlice> sourceSlices) {
        int workerCount = Math.max(1, Math.min(config.threads(), Math.max(1, sourceSlices.size())));
        List<SourcePartitionWorkBuilder> buckets = new ArrayList<>(workerCount);
        for (int workerId = 0; workerId < workerCount; workerId++) {
            buckets.add(new SourcePartitionWorkBuilder(workerId));
        }

        List<SourcePartitionSlice> slices = new ArrayList<>(sourceSlices);
        slices.sort(Comparator
                .comparingLong(SourcePartitionSlice::bytes)
                .reversed()
                .thenComparingInt(SourcePartitionSlice::partition)
                .thenComparingLong(SourcePartitionSlice::startByte));

        for (SourcePartitionSlice slice : slices) {
            SourcePartitionWorkBuilder lightest = buckets.get(0);
            for (SourcePartitionWorkBuilder bucket : buckets) {
                if (bucket.bytes() < lightest.bytes()) {
                    lightest = bucket;
                }
            }
            lightest.add(slice);
        }

        List<SourcePartitionWork> work = new ArrayList<>(workerCount);
        for (SourcePartitionWorkBuilder bucket : buckets) {
            if (!bucket.slices().isEmpty()) {
                work.add(bucket.build());
            }
        }
        return work;
    }

    private List<SourcePartitionSlice> readNonEmptySourcePartitionSlices(PreprocessingResult graph) throws IOException {
        Path metadataPath = graph.edgesDir().resolve("source-partitions.tsv");
        List<SourcePartitionSlice> slices = new ArrayList<>();
        try (var reader = Files.newBufferedReader(metadataPath, StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            if (line == null) {
                return slices;
            }
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split("\t", -1);
                if (parts.length < 3) {
                    throw new IOException("invalid source partition metadata line: " + line);
                }
                int partition = Integer.parseInt(parts[0]);
                long bytes = Long.parseLong(parts[2]);
                if (bytes > 0) {
                    addSourcePartitionSlices(slices, partition, bytes);
                }
            }
        }
        return slices;
    }

    private void addSourcePartitionSlices(List<SourcePartitionSlice> slices, int partition, long bytes) throws IOException {
        if (bytes % EDGE_RECORD_BYTES != 0) {
            throw new IOException("source partition byte size is not edge-record aligned: partition=%d bytes=%d"
                    .formatted(partition, bytes));
        }
        long startByte = 0;
        while (startByte < bytes) {
            long remaining = bytes - startByte;
            long sliceBytes = Math.min(remaining, config.scatterSliceBytes());
            sliceBytes -= sliceBytes % EDGE_RECORD_BYTES;
            if (sliceBytes == 0) {
                sliceBytes = EDGE_RECORD_BYTES;
            }
            slices.add(new SourcePartitionSlice(partition, startByte, sliceBytes));
            startByte += sliceBytes;
        }
    }

    private List<Integer> bucketsByDescendingMessageBytes(
            MessageBucketLayout bucketLayout,
            Map<Integer, List<MessageFile>> messageIndex
    ) {
        List<Integer> buckets = new ArrayList<>(bucketLayout.bucketCount());
        for (int bucket = 0; bucket < bucketLayout.bucketCount(); bucket++) {
            buckets.add(bucket);
        }
        buckets.sort(Comparator
                .comparingLong((Integer bucket) -> messageIndex
                        .getOrDefault(bucket, List.of())
                        .stream()
                        .mapToLong(MessageFile::bytes)
                        .sum())
                .reversed()
                .thenComparingInt(Integer::intValue));
        return buckets;
    }

    private Map<Integer, List<MessageFile>> buildMessageBucketIndex(Path iterationMessagesDir) throws IOException {
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
                try (var reader = Files.newBufferedReader(manifest, StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) {
                            continue;
                        }
                        int bucket = Integer.parseInt(line.trim());
                        Path messagePath = workerDir.resolve("msg-bucket-%05d.bin".formatted(bucket));
                        long bytes = Files.size(messagePath);
                        index.computeIfAbsent(bucket, ignored -> new ArrayList<>())
                                .add(new MessageFile(messagePath, bytes));
                    }
                }
            }
        }
        return index;
    }

    private void validateMessageBuckets(
            Map<Integer, List<MessageFile>> messageIndex,
            MessageBucketLayout bucketLayout
    ) throws IOException {
        for (int bucket : messageIndex.keySet()) {
            if (bucket < 0 || bucket >= bucketLayout.bucketCount()) {
                throw new IOException("message manifest references invalid bucket: " + bucket);
            }
        }
    }

    private long messageBytesFromManifest(Path workerDir) throws IOException {
        long bytes = 0;
        Path manifest = workerDir.resolve("manifest.txt");
        if (!Files.exists(manifest)) {
            return 0;
        }
        try (var reader = Files.newBufferedReader(manifest, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                int bucket = Integer.parseInt(line.trim());
                bytes += Files.size(workerDir.resolve("msg-bucket-%05d.bin".formatted(bucket)));
            }
        }
        return bytes;
    }

    private ConvergenceStats calculateConvergence(PreprocessingResult graph, ExecutorService executor) throws IOException {
        List<ChunkRange> ranges = chunkRanges(graph);
        List<Callable<ConvergenceStats>> tasks = new ArrayList<>(ranges.size());
        for (ChunkRange range : ranges) {
            tasks.add(() -> calculateConvergenceRange(graph, range));
        }
        double l1Diff = 0.0;
        double rankSum = 0.0;
        for (ConvergenceStats stats : invokeAll(executor, tasks)) {
            l1Diff += stats.l1Diff();
            rankSum += stats.rankSum();
        }
        return new ConvergenceStats(l1Diff, rankSum);
    }

    private ConvergenceStats calculateConvergenceRange(PreprocessingResult graph, ChunkRange range) throws IOException {
        double l1Diff = 0.0;
        double rankSum = 0.0;
        try (DiskDoubleArray current = new DiskDoubleArray(graph.rankCurrentPath(), graph.vertexCount(), graph.chunkSize(), false);
             DiskDoubleArray next = new DiskDoubleArray(graph.rankNextPath(), graph.vertexCount(), graph.chunkSize(), false)) {
            for (long start = range.startId(); start < range.endIdExclusive(); start += graph.chunkSize()) {
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

    private List<ChunkRange> chunkRanges(PreprocessingResult graph) {
        long chunkCount = Math.ceilDiv(graph.vertexCount(), graph.chunkSize());
        int rangeCount = Math.toIntExact(Math.min((long) config.threads(), Math.max(1L, chunkCount)));
        List<ChunkRange> ranges = new ArrayList<>(rangeCount);
        long chunksPerRange = Math.ceilDiv(chunkCount, rangeCount);
        for (int range = 0; range < rangeCount; range++) {
            long startChunk = range * chunksPerRange;
            long endChunk = Math.min(chunkCount, startChunk + chunksPerRange);
            if (startChunk < endChunk) {
                ranges.add(new ChunkRange(
                        startChunk * (long) graph.chunkSize(),
                        Math.min(graph.vertexCount(), endChunk * (long) graph.chunkSize())
                ));
            }
        }
        return ranges;
    }

    private void swapRankFiles(Path currentPath, Path nextPath) throws IOException {
        Path tmpPath = currentPath.resolveSibling("rank-swap-%d.tmp".formatted(System.nanoTime()));
        boolean currentMovedToTmp = false;
        boolean nextMovedToCurrent = false;
        try {
            moveReplacing(currentPath, tmpPath);
            currentMovedToTmp = true;
            moveReplacing(nextPath, currentPath);
            nextMovedToCurrent = true;
            moveReplacing(tmpPath, nextPath);
        } catch (IOException ex) {
            IOException rollbackFailure = rollbackRankSwap(
                    currentPath,
                    nextPath,
                    tmpPath,
                    currentMovedToTmp,
                    nextMovedToCurrent
            );
            if (rollbackFailure != null) {
                ex.addSuppressed(rollbackFailure);
            }
            throw ex;
        } finally {
            if (Files.exists(tmpPath)) {
                Files.deleteIfExists(tmpPath);
            }
        }
    }

    private void moveReplacing(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private IOException rollbackRankSwap(
            Path currentPath,
            Path nextPath,
            Path tmpPath,
            boolean currentMovedToTmp,
            boolean nextMovedToCurrent
    ) {
        IOException failure = null;
        try {
            if (nextMovedToCurrent && Files.exists(currentPath) && !Files.exists(nextPath)) {
                moveReplacing(currentPath, nextPath);
            }
            if (currentMovedToTmp && Files.exists(tmpPath)) {
                moveReplacing(tmpPath, currentPath);
            }
        } catch (IOException rollbackEx) {
            failure = rollbackEx;
        }
        return failure;
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

    private void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("unexpected EOF while reading edge partition");
            }
            if (read == 0) {
                throw new IOException("zero-byte read while reading edge partition");
            }
            position += read;
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
        FileUtils.deleteRecursively(path);
    }

    private final class GatherChunkCache {
        private final PreprocessingResult graph;
        private final DiskDoubleArray nextRanks;
        private final double base;
        private final int maxChunks;
        private final LinkedHashMap<Integer, CachedGatherChunk> chunks =
                new LinkedHashMap<>(16, 0.75f, true);
        private final Set<Integer> writtenPartitions = new HashSet<>();

        private GatherChunkCache(PreprocessingResult graph, DiskDoubleArray nextRanks, double base, int maxChunks) {
            this.graph = graph;
            this.nextRanks = nextRanks;
            this.base = base;
            this.maxChunks = maxChunks;
        }

        private CachedGatherChunk chunk(int destinationPartition) throws IOException {
            CachedGatherChunk cached = chunks.get(destinationPartition);
            if (cached != null) {
                return cached;
            }
            evictIfNeeded();
            long start = (long) destinationPartition * graph.chunkSize();
            int length = chunkLength(graph, start);
            ByteBuffer scratch = ByteBuffer.allocate(length * Double.BYTES);
            double[] values;
            if (writtenPartitions.contains(destinationPartition)) {
                values = nextRanks.readChunk(start, length, scratch);
            } else {
                values = new double[length];
                Arrays.fill(values, base);
            }
            cached = new CachedGatherChunk(destinationPartition, start, length, values, scratch);
            chunks.put(destinationPartition, cached);
            return cached;
        }

        private void evictIfNeeded() throws IOException {
            if (chunks.size() < maxChunks) {
                return;
            }
            var iterator = chunks.entrySet().iterator();
            if (iterator.hasNext()) {
                CachedGatherChunk eldest = iterator.next().getValue();
                write(eldest);
                iterator.remove();
            }
        }

        private void flushRange(int firstPartition, int endPartitionExclusive) throws IOException {
            for (int partition = firstPartition; partition < endPartitionExclusive; partition++) {
                CachedGatherChunk chunk = chunks.remove(partition);
                if (chunk != null) {
                    write(chunk);
                } else if (!writtenPartitions.contains(partition)) {
                    writeBaseChunk(partition);
                }
            }
            for (CachedGatherChunk chunk : chunks.values()) {
                throw new IOException("cached gather chunk outside bucket range: partition=" + chunk.partition());
            }
            chunks.clear();
        }

        private void write(CachedGatherChunk chunk) throws IOException {
            nextRanks.writeChunk(chunk.start(), chunk.values(), chunk.length(), chunk.scratch());
            writtenPartitions.add(chunk.partition());
        }

        private void writeBaseChunk(int partition) throws IOException {
            long start = (long) partition * graph.chunkSize();
            int length = chunkLength(graph, start);
            double[] values = new double[length];
            Arrays.fill(values, base);
            nextRanks.writeChunk(start, values, length, ByteBuffer.allocate(length * Double.BYTES));
            writtenPartitions.add(partition);
        }
    }

    public record PageRankRunResult(
            Path rankPath,
            Path verticesPath,
            long vertexCount,
            long edgeCount,
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

    private record ChunkRange(long startId, long endIdExclusive) {
    }

    private record SourcePartitionWork(int workerId, List<SourcePartitionSlice> slices, long bytes) {
    }

    private record SourcePartitionSlice(int partition, long startByte, long bytes) {
    }

    private record CachedGatherChunk(int partition, long start, int length, double[] values, ByteBuffer scratch) {
    }

    private static final class SourcePartitionWorkBuilder {
        private final int workerId;
        private final List<SourcePartitionSlice> slices = new ArrayList<>();
        private long bytes;

        private SourcePartitionWorkBuilder(int workerId) {
            this.workerId = workerId;
        }

        private void add(SourcePartitionSlice slice) {
            slices.add(slice);
            bytes += slice.bytes();
        }

        private int workerId() {
            return workerId;
        }

        private List<SourcePartitionSlice> slices() {
            return slices;
        }

        private long bytes() {
            return bytes;
        }

        private SourcePartitionWork build() {
            return new SourcePartitionWork(workerId, List.copyOf(slices), bytes);
        }
    }
}
