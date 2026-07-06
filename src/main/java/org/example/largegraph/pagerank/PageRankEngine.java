package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.graph.GraphPreprocessor.PreprocessingResult;
import org.example.largegraph.io.BinaryMessageReader;
import org.example.largegraph.io.BinaryMessageReader.Message;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    private static final int EDGE_RECORD_BYTES = Integer.BYTES * 2;
    private static final long MAX_SCATTER_SLICE_BYTES = 1024L * 1024L;
    private static final int EDGE_READ_BUFFER_BYTES = 64 * 1024;
    private static final int MAX_GATHER_CHUNK_CACHE = 2;

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

                double danglingMass = calculateDanglingMass(graph);
                scatter(graph, iterationMessagesDir, executor, scatterWork);
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
        int maxOpenMessageWriters = Math.min(64, Math.max(4, config.threads() * 4));

        try (DiskDoubleArray ranks = new DiskDoubleArray(graph.rankCurrentPath(), graph.vertexCount(), graph.chunkSize(), false);
             DiskIntArray outDegree = new DiskIntArray(graph.outDegreePath(), graph.vertexCount(), graph.chunkSize(), false);
             MessagePartitionWriterManager messageWriters =
                     new MessagePartitionWriterManager(workerDir, graph.destinationPartitionCount(), maxOpenMessageWriters)) {
            int cachedSourcePartition = -1;
            long cachedSourceStart = -1;
            double[] cachedRankChunk = null;
            int[] cachedOutDegreeChunk = null;
            for (SourcePartitionSlice slice : work.slices()) {
                int sourcePartition = slice.partition();
                if (sourcePartition != cachedSourcePartition) {
                    cachedSourcePartition = sourcePartition;
                    cachedSourceStart = (long) sourcePartition * graph.chunkSize();
                    int sourceLength = chunkLength(graph, cachedSourceStart);
                    cachedRankChunk = ranks.readChunk(cachedSourceStart, sourceLength);
                    cachedOutDegreeChunk = outDegree.readIntChunk(cachedSourceStart, sourceLength);
                }
                Path sourcePartitionPath = graph.edgesDir()
                        .resolve("src-part-%05d.bin".formatted(sourcePartition));
                if (!Files.exists(sourcePartitionPath)) {
                    continue;
                }

                ScatterStats sliceStats = scatterSourcePartitionSlice(
                        graph,
                        messageWriters,
                        sourcePartitionPath,
                        slice,
                        cachedSourceStart,
                        cachedRankChunk,
                        cachedOutDegreeChunk
                );
                edgeCount += sliceStats.edgeCount();
                messageCount += sliceStats.messageCount();
            }
        }
        long messageBytes = messageBytesFromManifest(workerDir);
        return new ScatterStats(edgeCount, messageCount, messageBytes);
    }

    private ScatterStats scatterSourcePartitionSlice(
            PreprocessingResult graph,
            MessagePartitionWriterManager messageWriters,
            Path sourcePartitionPath,
            SourcePartitionSlice slice,
            long sourceStart,
            double[] rankChunk,
            int[] outDegreeChunk
    ) throws IOException {
        long edgeCount = 0;
        long messageCount = 0;
        ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(EDGE_READ_BUFFER_BYTES, Math.max(EDGE_RECORD_BYTES, slice.bytes())));

        try (FileChannel channel = FileChannel.open(sourcePartitionPath, StandardOpenOption.READ)) {
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
        fillRankNextWithBase(graph, base);

        List<Callable<GatherStats>> tasks = new ArrayList<>(messageIndex.size());
        for (int bucket : touchedBucketsByDescendingMessageBytes(messageIndex)) {
            List<MessageFile> messageFiles = messageIndex.get(bucket);
            tasks.add(() -> gatherBucket(graph, messageFiles));
        }
        List<GatherStats> stats = invokeAll(executor, tasks);
        long messages = 0;
        for (GatherStats stat : stats) {
            messages += stat.messageCount();
        }
        logger.info("gather messages=%d tasks=%d".formatted(messages, tasks.size()));
    }

    private void fillRankNextWithBase(PreprocessingResult graph, double base) throws IOException {
        try (DiskDoubleArray nextRanks = new DiskDoubleArray(graph.rankNextPath(), graph.vertexCount(), graph.chunkSize())) {
            int maxChunkLength = Math.toIntExact(Math.min((long) graph.chunkSize(), Math.max(1L, graph.vertexCount())));
            double[] nextChunk = new double[maxChunkLength];
            ByteBuffer scratch = ByteBuffer.allocate(nextChunk.length * Double.BYTES);
            for (long start = 0; start < graph.vertexCount(); start += graph.chunkSize()) {
                int length = chunkLength(graph, start);
                java.util.Arrays.fill(nextChunk, 0, length, base);
                nextRanks.writeChunk(start, nextChunk, length, scratch);
            }
            nextRanks.flush();
        }
    }

    private GatherStats gatherBucket(
            PreprocessingResult graph,
            List<MessageFile> messageFiles
    ) throws IOException {
        long messageCount = 0;

        try (DiskDoubleArray nextRanks = new DiskDoubleArray(graph.rankNextPath(), graph.vertexCount(), graph.chunkSize())) {
            GatherChunkCache chunkCache = new GatherChunkCache(graph, nextRanks);
            for (MessageFile messageFile : messageFiles) {
                try (BinaryMessageReader reader = new BinaryMessageReader(messageFile.path())) {
                    Optional<Message> message;
                    while ((message = reader.next()).isPresent()) {
                        int to = message.get().to();
                        int destinationPartition = to / graph.chunkSize();
                        CachedGatherChunk chunk = chunkCache.chunk(destinationPartition);
                        int localTo = Math.toIntExact(to - chunk.start());
                        chunk.values()[localTo] += message.get().contribution();
                        messageCount++;
                    }
                }
            }
            chunkCache.flushAll();
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
            long sliceBytes = Math.min(remaining, MAX_SCATTER_SLICE_BYTES);
            sliceBytes -= sliceBytes % EDGE_RECORD_BYTES;
            if (sliceBytes == 0) {
                sliceBytes = EDGE_RECORD_BYTES;
            }
            slices.add(new SourcePartitionSlice(partition, startByte, sliceBytes));
            startByte += sliceBytes;
        }
    }

    private List<Integer> touchedBucketsByDescendingMessageBytes(Map<Integer, List<MessageFile>> messageIndex) {
        List<Integer> buckets = new ArrayList<>(messageIndex.keySet());
        buckets.sort(Comparator
                .comparingLong((Integer bucket) -> messageIndex
                        .get(bucket)
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

    private void readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("unexpected EOF while reading edge partition");
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
        private final LinkedHashMap<Integer, CachedGatherChunk> chunks =
                new LinkedHashMap<>(16, 0.75f, true);

        private GatherChunkCache(PreprocessingResult graph, DiskDoubleArray nextRanks) {
            this.graph = graph;
            this.nextRanks = nextRanks;
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
            cached = new CachedGatherChunk(start, length, nextRanks.readChunk(start, length, scratch), scratch);
            chunks.put(destinationPartition, cached);
            return cached;
        }

        private void evictIfNeeded() throws IOException {
            if (chunks.size() < MAX_GATHER_CHUNK_CACHE) {
                return;
            }
            var iterator = chunks.entrySet().iterator();
            if (iterator.hasNext()) {
                CachedGatherChunk eldest = iterator.next().getValue();
                write(eldest);
                iterator.remove();
            }
        }

        private void flushAll() throws IOException {
            for (CachedGatherChunk chunk : chunks.values()) {
                write(chunk);
            }
            chunks.clear();
        }

        private void write(CachedGatherChunk chunk) throws IOException {
            nextRanks.writeChunk(chunk.start(), chunk.values(), chunk.length(), chunk.scratch());
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

    private record SourcePartitionWork(int workerId, List<SourcePartitionSlice> slices, long bytes) {
    }

    private record SourcePartitionSlice(int partition, long startByte, long bytes) {
    }

    private record CachedGatherChunk(long start, int length, double[] values, ByteBuffer scratch) {
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
