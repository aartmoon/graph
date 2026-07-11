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
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    private final AppConfig config;
    private final ProgressLogger logger;

    public PageRankEngine(AppConfig config, ProgressLogger logger) {
        this.config = config;
        this.logger = logger;
    }

    public PageRankRunResult run(PreprocessingResult graph) throws IOException {
        int workerCount = plannedPageRankThreads(graph);
        List<SourcePartitionSlice> sourceSlices = readNonEmptySourcePartitionSlices(graph);
        validateSourceSliceCoverage(sourceSlices, graph.edgeCount());
        List<SourcePartitionWork> scatterWork = balancedSourcePartitionBuckets(sourceSlices, workerCount);
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        double lastDiff = Double.POSITIVE_INFINITY;
        double lastRankSum = Double.NaN;
        int completedIterations = 0;

        try {
            for (int iteration = 1; iteration <= config.maxIterations(); iteration++) {
                long startedAt = System.nanoTime();
                Path iterationMessagesDir = messagesDir(iteration);
                deleteRecursively(iterationMessagesDir);
                checkMessageDiskSpace(graph);
                Files.createDirectories(iterationMessagesDir);

                double danglingMass = calculateDanglingMass(graph, executor);
                double base = (1.0 - config.damping()) / graph.vertexCount()
                        + config.damping() * danglingMass / graph.vertexCount();
                ScatterStats scatterStats = scatter(graph, iterationMessagesDir, executor, scatterWork, workerCount);
                validateScatterStats(scatterStats, graph.edgeCount());
                GatherStats gatherStats = gather(graph, iterationMessagesDir, base, executor);
                if (gatherStats.messageCount() != scatterStats.messageCount()) {
                    throw new IOException("gather message count mismatch: expected=%d actual=%d"
                            .formatted(scatterStats.messageCount(), gatherStats.messageCount()));
                }

                ConvergenceStats stats = calculateConvergence(graph, executor);
                lastDiff = stats.l1Diff();
                lastRankSum = stats.rankSum();
                validateConvergenceStats(stats);

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

    private int plannedPageRankThreads(PreprocessingResult graph) {
        long maxHeap = MemoryUtils.maxHeapBytes();
        long availableHeap = Math.max(1L, maxHeap * 65L / 100L);
        long scatterTaskBytes = safeMultiply(graph.chunkSize(), 24L);
        long convergenceTaskBytes = safeMultiply(graph.chunkSize(), 32L);
        long gatherTaskBytes = safeAdd(safeMultiply(maxGatherBucketVertices(graph), 8L), 256L * 1024L);
        long perTaskBytes = Math.max(scatterTaskBytes, Math.max(convergenceTaskBytes, gatherTaskBytes));
        if (perTaskBytes > availableHeap) {
            throw new IllegalArgumentException("memory configuration cannot fit one PageRank task: availableHeap=%s requiredPerTask=%s"
                    .formatted(
                            MemoryUtils.humanReadableBytes(availableHeap),
                            MemoryUtils.humanReadableBytes(perTaskBytes)
                    ));
        }
        int safeParallelism = Math.max(1, (int) Math.min(config.threads(), availableHeap / perTaskBytes));
        if (safeParallelism < config.threads()) {
            logger.info("Reducing PageRank parallelism from %d to %d for heap budget"
                    .formatted(config.threads(), safeParallelism));
        }
        return safeParallelism;
    }

    private long maxGatherBucketVertices(PreprocessingResult graph) {
        MessageBucketLayout layout = new MessageBucketLayout(
                graph.destinationPartitionCount(),
                graph.chunkSize(),
                graph.vertexCount()
        );
        return layout.verticesPerBucket();
    }

    private void checkMessageDiskSpace(PreprocessingResult graph) throws IOException {
        FileStore store = Files.getFileStore(config.workDir());
        long free = store.getUsableSpace();
        long messageBytes = safeMultiply(graph.edgeCount(), Integer.BYTES + Double.BYTES);
        long reserveBytes = 64L * 1024L * 1024L;
        long required = safeAdd(messageBytes, reserveBytes);
        if (free < required) {
            throw new IOException("not enough disk space for PageRank messages: free=%d estimatedRequired=%d"
                    .formatted(free, required));
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
            double[] rankChunk = new double[graph.chunkSize()];
            int[] degreeChunk = new int[graph.chunkSize()];
            ByteBuffer rankScratch = ByteBuffer.allocate(graph.chunkSize() * Double.BYTES);
            ByteBuffer degreeScratch = ByteBuffer.allocate(graph.chunkSize() * Integer.BYTES);
            for (long start = range.startId(); start < range.endIdExclusive(); start += graph.chunkSize()) {
                int length = chunkLength(graph, start);
                ranks.readChunk(start, rankChunk, 0, length, rankScratch);
                outDegree.readIntChunk(start, degreeChunk, 0, length, degreeScratch);
                for (int i = 0; i < length; i++) {
                    if (degreeChunk[i] == 0) {
                        danglingMass += rankChunk[i];
                    }
                }
            }
        }
        return danglingMass;
    }

    private ScatterStats scatter(
            PreprocessingResult graph,
            Path iterationMessagesDir,
            ExecutorService executor,
            List<SourcePartitionWork> workerBuckets,
            int workerCount
    ) throws IOException {
        List<Callable<ScatterStats>> tasks = new ArrayList<>(workerBuckets.size());
        for (SourcePartitionWork work : workerBuckets) {
            tasks.add(() -> scatterSourcePartitions(graph, iterationMessagesDir, work, workerCount));
        }
        List<ScatterStats> stats = invokeAll(executor, tasks);
        long edges = 0;
        long messages = 0;
        long bytes = 0;
        for (ScatterStats stat : stats) {
            edges += stat.edgeCount();
            messages += stat.messageCount();
            bytes += stat.messageBytes();
        }
        logger.info("scatter messages=%d bytes=%d tasks=%d".formatted(messages, bytes, tasks.size()));
        return new ScatterStats(edges, messages, bytes);
    }

    private ScatterStats scatterSourcePartitions(
            PreprocessingResult graph,
            Path iterationMessagesDir,
            SourcePartitionWork work,
            int workerCount
    ) throws IOException {
        long edgeCount = 0;
        long messageCount = 0;
        Path workerDir = iterationMessagesDir.resolve("worker-%05d".formatted(work.workerId()));
        int maxOpenMessageWriters = maxOpenMessageWritersPerWorker(workerCount);

        try (DiskDoubleArray ranks = new DiskDoubleArray(graph.rankCurrentPath(), graph.vertexCount(), graph.chunkSize(), false);
             DiskIntArray outDegree = new DiskIntArray(graph.outDegreePath(), graph.vertexCount(), graph.chunkSize(), false);
             MessagePartitionWriterManager messageWriters =
                     new MessagePartitionWriterManager(
                             workerDir,
                             graph.destinationPartitionCount(),
                             graph.chunkSize(),
                             graph.vertexCount(),
                             maxOpenMessageWriters
                     )) {
            Map<Integer, List<SourcePartitionSlice>> slicesByPartition = slicesBySourcePartition(work.slices());
            double[] rankChunk = new double[graph.chunkSize()];
            int[] outDegreeChunk = new int[graph.chunkSize()];
            ByteBuffer rankScratch = ByteBuffer.allocate(graph.chunkSize() * Double.BYTES);
            ByteBuffer degreeScratch = ByteBuffer.allocate(graph.chunkSize() * Integer.BYTES);
            for (Map.Entry<Integer, List<SourcePartitionSlice>> entry : slicesByPartition.entrySet()) {
                int sourcePartition = entry.getKey();
                long sourceStart = (long) sourcePartition * graph.chunkSize();
                int sourceLength = chunkLength(graph, sourceStart);
                ranks.readChunk(sourceStart, rankChunk, 0, sourceLength, rankScratch);
                outDegree.readIntChunk(sourceStart, outDegreeChunk, 0, sourceLength, degreeScratch);
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

    private int maxOpenMessageWritersPerWorker(int workerCount) {
        int totalBudget = Math.max(workerCount, Math.min(MAX_TOTAL_MESSAGE_WRITERS, workerCount * 8));
        return Math.max(1, totalBudget / workerCount);
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

    private GatherStats gather(
            PreprocessingResult graph,
            Path iterationMessagesDir,
            double base,
            ExecutorService executor
    ) throws IOException {
        Map<Integer, List<MessageFile>> messageIndex = buildMessageBucketIndex(iterationMessagesDir);
        MessageBucketLayout bucketLayout = new MessageBucketLayout(
                graph.destinationPartitionCount(),
                graph.chunkSize(),
                graph.vertexCount()
        );
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
        return new GatherStats(messages);
    }

    private void addGatherTasks(
            List<Callable<GatherStats>> tasks,
            PreprocessingResult graph,
            MessageBucketLayout bucketLayout,
            int bucket,
            List<MessageFile> messageFiles,
            double base
    ) {
        tasks.add(() -> gatherBucketRange(
                graph,
                bucket,
                bucketLayout.firstDenseVertexId(bucket),
                bucketLayout.endDenseVertexIdExclusive(bucket),
                messageFiles,
                base
        ));
    }

    private GatherStats gatherBucketRange(
            PreprocessingResult graph,
            int bucket,
            long rangeStart,
            long rangeEndExclusive,
            List<MessageFile> messageFiles,
            double base
    ) throws IOException {
        long messageCount = 0;
        int length = Math.toIntExact(rangeEndExclusive - rangeStart);
        double[] values = new double[length];
        Arrays.fill(values, base);

        try (DiskDoubleArray nextRanks = new DiskDoubleArray(graph.rankNextPath(), graph.vertexCount(), graph.chunkSize(), true, false)) {
            for (MessageFile messageFile : messageFiles) {
                try (BinaryMessageReader reader = new BinaryMessageReader(messageFile.path())) {
                    while (reader.next()) {
                        int to = reader.to();
                        if (to < rangeStart || to >= rangeEndExclusive) {
                            throw new IOException("message destination does not belong to bucket: bucket=%d vertex=%d range=[%d,%d)"
                                    .formatted(bucket, to, rangeStart, rangeEndExclusive));
                        }
                        values[Math.toIntExact(to - rangeStart)] += reader.contribution();
                        if (!Double.isFinite(values[Math.toIntExact(to - rangeStart)])) {
                            throw new IOException("non-finite rank contribution while gathering bucket: " + bucket);
                        }
                        messageCount++;
                    }
                }
            }
            nextRanks.writeChunk(rangeStart, values, length);
        }
        return new GatherStats(messageCount);
    }

    private List<SourcePartitionWork> balancedSourcePartitionBuckets(List<SourcePartitionSlice> sourceSlices, int maxWorkers) {
        int workerCount = Math.max(1, Math.min(maxWorkers, Math.max(1, sourceSlices.size())));
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
                    throw new IOException("missing message manifest: " + manifest);
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
                        if (bytes % (Integer.BYTES + Double.BYTES) != 0) {
                            throw new IOException("message file is not record-aligned: " + messagePath);
                        }
                        index.computeIfAbsent(bucket, ignored -> new ArrayList<>())
                                .add(new MessageFile(messagePath, bytes));
                    }
                }
            }
        }
        return index;
    }

    private void validateSourceSliceCoverage(List<SourcePartitionSlice> sourceSlices, long expectedEdges) throws IOException {
        long bytes = 0;
        for (SourcePartitionSlice slice : sourceSlices) {
            bytes += slice.bytes();
        }
        long edges = bytes / EDGE_RECORD_BYTES;
        if (edges != expectedEdges) {
            throw new IOException("source partition metadata edge count mismatch: expected=%d actual=%d"
                    .formatted(expectedEdges, edges));
        }
    }

    private void validateScatterStats(ScatterStats stats, long expectedEdges) throws IOException {
        if (stats.edgeCount() != expectedEdges) {
            throw new IOException("scatter edge count mismatch: expected=%d actual=%d"
                    .formatted(expectedEdges, stats.edgeCount()));
        }
        if (stats.messageCount() != expectedEdges) {
            throw new IOException("scatter message count mismatch: expected=%d actual=%d"
                    .formatted(expectedEdges, stats.messageCount()));
        }
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
            double[] currentChunk = new double[graph.chunkSize()];
            double[] nextChunk = new double[graph.chunkSize()];
            ByteBuffer currentScratch = ByteBuffer.allocate(graph.chunkSize() * Double.BYTES);
            ByteBuffer nextScratch = ByteBuffer.allocate(graph.chunkSize() * Double.BYTES);
            for (long start = range.startId(); start < range.endIdExclusive(); start += graph.chunkSize()) {
                int length = chunkLength(graph, start);
                current.readChunk(start, currentChunk, 0, length, currentScratch);
                next.readChunk(start, nextChunk, 0, length, nextScratch);
                for (int i = 0; i < length; i++) {
                    if (!Double.isFinite(nextChunk[i]) || nextChunk[i] < 0.0) {
                        throw new IOException("invalid rank value at dense vertex %d: %.12g"
                                .formatted(start + i, nextChunk[i]));
                    }
                    l1Diff += Math.abs(nextChunk[i] - currentChunk[i]);
                    rankSum += nextChunk[i];
                }
            }
        }
        return new ConvergenceStats(l1Diff, rankSum);
    }

    private void validateConvergenceStats(ConvergenceStats stats) throws IOException {
        if (!Double.isFinite(stats.l1Diff()) || !Double.isFinite(stats.rankSum())) {
            throw new IOException("non-finite convergence stats: diff=%s rankSum=%s"
                    .formatted(stats.l1Diff(), stats.rankSum()));
        }
        if (Math.abs(stats.rankSum() - 1.0) > RANK_SUM_WARNING_THRESHOLD) {
            throw new IOException("rankSum invariant failed: %.12g".formatted(stats.rankSum()));
        }
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

    private long safeMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

    private long safeAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
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
