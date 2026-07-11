package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.graph.GraphPreprocessor.PreprocessingResult;
import org.example.largegraph.io.BinaryMessageReader;
import org.example.largegraph.io.MessageBucketLayout;
import org.example.largegraph.io.MessagePartitionWriterManager;
import org.example.largegraph.io.MessagePartitionWriterManager.MessageFileInfo;
import org.example.largegraph.storage.DiskDoubleArray;
import org.example.largegraph.storage.DiskIntArray;
import org.example.largegraph.util.FileUtils;
import org.example.largegraph.util.MemoryUtils;
import org.example.largegraph.util.ProgressLogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
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
    private static final int MAX_TOTAL_MESSAGE_FILES = 16_384;
    private static final long ESTIMATED_FILE_BLOCK_BYTES = 4_096L;

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
        workerCount = Math.min(workerCount, Math.max(1, sourceSlices.size()));
        List<SourcePartitionWork> scatterWork = balancedSourcePartitionBuckets(sourceSlices, workerCount);
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        Path currentRankPath = graph.rankCurrentPath();
        Path nextRankPath = graph.rankNextPath();
        double lastDiff = Double.POSITIVE_INFINITY;
        double lastRankSum = Double.NaN;
        int completedIterations = 0;

        try {
            for (int iteration = 1; iteration <= config.maxIterations(); iteration++) {
                long startedAt = System.nanoTime();
                Path iterationMessagesDir = messagesDir(iteration);
                deleteRecursively(iterationMessagesDir);
                checkMessageDiskSpace(graph, workerCount);
                IOException primaryFailure = null;
                try {
                    Files.createDirectories(iterationMessagesDir);

                    double danglingMass = calculateDanglingMass(graph, currentRankPath, executor, workerCount);
                    double base = (1.0 - config.damping()) / graph.vertexCount()
                            + config.damping() * danglingMass / graph.vertexCount();
                    ScatterStats scatterStats = scatter(graph, currentRankPath, iterationMessagesDir, executor, scatterWork, workerCount);
                    validateScatterStats(scatterStats, graph.edgeCount());
                    GatherStats gatherStats = gather(graph, currentRankPath, nextRankPath, scatterStats.messageFiles(), base, executor);
                    if (gatherStats.messageCount() != scatterStats.messageCount()) {
                        throw new IOException("gather message count mismatch: expected=%d actual=%d"
                                .formatted(scatterStats.messageCount(), gatherStats.messageCount()));
                    }
                    lastDiff = gatherStats.l1Diff();
                    lastRankSum = gatherStats.rankSum();
                    validateGatherStats(gatherStats);

                    Path tmp = currentRankPath;
                    currentRankPath = nextRankPath;
                    nextRankPath = tmp;
                } catch (IOException ex) {
                    primaryFailure = ex;
                    throw ex;
                } finally {
                    cleanupMessages(iterationMessagesDir, primaryFailure);
                }
                completedIterations = iteration;
                logIteration(iteration, lastDiff, lastRankSum, startedAt);

                if (lastDiff < config.epsilon()) {
                    return new PageRankRunResult(
                            currentRankPath,
                            graph.verticesPath(),
                            graph.vertexCount(),
                            graph.edgeCount(),
                            completedIterations,
                            lastDiff,
                            lastRankSum
                    );
                }
            }

            throw new IOException("PageRank did not converge after %d iterations: diff=%.12g epsilon=%.12g"
                    .formatted(config.maxIterations(), lastDiff, config.epsilon()));
        } finally {
            shutdown(executor);
        }
    }

    private int plannedPageRankThreads(PreprocessingResult graph) throws IOException {
        long maxHeap = MemoryUtils.maxHeapBytes();
        long availableHeap = Math.max(1L, maxHeap * 65L / 100L);
        long scatterTaskBytes = safeMultiply(config.chunkSize(), 24L);
        long gatherTaskBytes = safeAdd(
                safeAdd(safeMultiply(maxGatherBucketVertices(graph), 8L), safeMultiply(config.chunkSize(), 16L)),
                256L * 1024L
        );
        long perTaskBytes = Math.max(scatterTaskBytes, gatherTaskBytes);
        if (perTaskBytes > availableHeap) {
            throw new IOException("memory configuration cannot fit one PageRank task: availableHeap=%s requiredPerTask=%s"
                    .formatted(
                            MemoryUtils.humanReadableBytes(availableHeap),
                            MemoryUtils.humanReadableBytes(perTaskBytes)
                    ));
        }
        MessageBucketLayout layout = new MessageBucketLayout(
                partitionCount(graph),
                config.chunkSize(),
                graph.vertexCount()
        );
        long maxWorkersByFiles = Math.max(1L, MAX_TOTAL_MESSAGE_FILES / layout.bucketCount());
        int cpuParallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
        int safeParallelism = Math.max(1, (int) Math.min(
                Math.min((long) config.threads(), cpuParallelism),
                Math.min(availableHeap / perTaskBytes, maxWorkersByFiles)
        ));
        if (safeParallelism < config.threads()) {
            logger.info("Reducing PageRank parallelism from %d to %d for heap budget"
                    .formatted(config.threads(), safeParallelism));
        }
        return safeParallelism;
    }

    private long maxGatherBucketVertices(PreprocessingResult graph) {
        MessageBucketLayout layout = new MessageBucketLayout(
                partitionCount(graph),
                config.chunkSize(),
                graph.vertexCount()
        );
        return layout.verticesPerBucket();
    }

    private int partitionCount(PreprocessingResult graph) {
        long partitions = Math.ceilDiv(graph.vertexCount(), config.chunkSize());
        if (partitions > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("too many partitions: " + partitions);
        }
        return Math.toIntExact(partitions);
    }

    private void checkMessageDiskSpace(PreprocessingResult graph, int workerCount) throws IOException {
        MessageBucketLayout layout = new MessageBucketLayout(
                partitionCount(graph),
                config.chunkSize(),
                graph.vertexCount()
        );
        FileStore store = Files.getFileStore(config.workDir());
        long free = store.getUsableSpace();
        long messageBytes = safeMultiply(graph.edgeCount(), Integer.BYTES + Double.BYTES);
        long fileBlockBytes = safeMultiply(safeMultiply(workerCount, layout.bucketCount()), ESTIMATED_FILE_BLOCK_BYTES);
        long reserveBytes = 64L * 1024L * 1024L;
        long required = safeAdd(safeAdd(messageBytes, fileBlockBytes), reserveBytes);
        if (free < required) {
            throw new IOException("not enough disk space for PageRank messages: free=%d estimatedRequired=%d"
                    .formatted(free, required));
        }
    }

    private double calculateDanglingMass(
            PreprocessingResult graph,
            Path currentRankPath,
            ExecutorService executor,
            int workerCount
    ) throws IOException {
        List<ChunkRange> ranges = chunkRanges(graph, workerCount);
        List<Callable<Double>> tasks = new ArrayList<>(ranges.size());
        for (ChunkRange range : ranges) {
            tasks.add(() -> calculateDanglingMassRange(graph, currentRankPath, range));
        }
        double danglingMass = 0.0;
        for (double localMass : invokeAll(executor, tasks)) {
            danglingMass += localMass;
        }
        return danglingMass;
    }

    private double calculateDanglingMassRange(
            PreprocessingResult graph,
            Path currentRankPath,
            ChunkRange range
    ) throws IOException {
        double danglingMass = 0.0;
        try (DiskDoubleArray ranks = new DiskDoubleArray(currentRankPath, graph.vertexCount(), config.chunkSize(), false);
             DiskIntArray outDegree = new DiskIntArray(graph.outDegreePath(), graph.vertexCount(), config.chunkSize(), false)) {
            double[] rankChunk = new double[config.chunkSize()];
            int[] degreeChunk = new int[config.chunkSize()];
            ByteBuffer rankScratch = ByteBuffer.allocate(config.chunkSize() * Double.BYTES);
            ByteBuffer degreeScratch = ByteBuffer.allocate(config.chunkSize() * Integer.BYTES);
            for (long start = range.startId(); start < range.endIdExclusive(); start += config.chunkSize()) {
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
            Path currentRankPath,
            Path iterationMessagesDir,
            ExecutorService executor,
            List<SourcePartitionWork> workerBuckets,
            int workerCount
    ) throws IOException {
        List<Callable<ScatterStats>> tasks = new ArrayList<>(workerBuckets.size());
        for (SourcePartitionWork work : workerBuckets) {
            tasks.add(() -> scatterSourcePartitions(graph, currentRankPath, iterationMessagesDir, work, workerCount));
        }
        List<ScatterStats> stats = invokeAll(executor, tasks);
        long edges = 0;
        long messages = 0;
        long bytes = 0;
        List<MessageFileInfo> messageFiles = new ArrayList<>();
        for (ScatterStats stat : stats) {
            edges += stat.edgeCount();
            messages += stat.messageCount();
            bytes += stat.messageBytes();
            messageFiles.addAll(stat.messageFiles());
        }
        logger.info("scatter messages=%d bytes=%d tasks=%d".formatted(messages, bytes, tasks.size()));
        return new ScatterStats(edges, messages, bytes, List.copyOf(messageFiles));
    }

    private ScatterStats scatterSourcePartitions(
            PreprocessingResult graph,
            Path currentRankPath,
            Path iterationMessagesDir,
            SourcePartitionWork work,
            int workerCount
    ) throws IOException {
        long edgeCount = 0;
        long messageCount = 0;
        Path workerDir = iterationMessagesDir.resolve("worker-%05d".formatted(work.workerId()));
        int maxOpenMessageWriters = maxOpenMessageWritersPerWorker(workerCount);
        List<MessageFileInfo> messageFiles;

        try (DiskDoubleArray ranks = new DiskDoubleArray(currentRankPath, graph.vertexCount(), config.chunkSize(), false);
             DiskIntArray outDegree = new DiskIntArray(graph.outDegreePath(), graph.vertexCount(), config.chunkSize(), false)) {
            MessagePartitionWriterManager messageWriters = new MessagePartitionWriterManager(
                    workerDir,
                    partitionCount(graph),
                    config.chunkSize(),
                    graph.vertexCount(),
                    maxOpenMessageWriters
            );
            boolean closed = false;
            try {
                Map<Integer, List<SourcePartitionSlice>> slicesByPartition = slicesBySourcePartition(work.slices());
                double[] rankChunk = new double[config.chunkSize()];
                int[] outDegreeChunk = new int[config.chunkSize()];
                ByteBuffer rankScratch = ByteBuffer.allocate(config.chunkSize() * Double.BYTES);
                ByteBuffer degreeScratch = ByteBuffer.allocate(config.chunkSize() * Integer.BYTES);
                for (Map.Entry<Integer, List<SourcePartitionSlice>> entry : slicesByPartition.entrySet()) {
                    int sourcePartition = entry.getKey();
                    long sourceStart = (long) sourcePartition * config.chunkSize();
                    int sourceLength = chunkLength(graph, sourceStart);
                    ranks.readChunk(sourceStart, rankChunk, 0, sourceLength, rankScratch);
                    outDegree.readIntChunk(sourceStart, outDegreeChunk, 0, sourceLength, degreeScratch);
                    Path sourcePartitionPath = graph.edgesDir()
                            .resolve("src-part-%05d.bin".formatted(sourcePartition));

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
                messageWriters.close();
                closed = true;
                messageFiles = messageWriters.messageFiles();
            } finally {
                if (!closed) {
                    try {
                        messageWriters.close();
                    } catch (IOException closeEx) {
                        logger.info("WARNING failed to close message writers: " + closeEx.getMessage());
                    }
                }
            }
        }
        long messageBytes = messageFiles.stream().mapToLong(MessageFileInfo::bytes).sum();
        return new ScatterStats(edgeCount, messageCount, messageBytes, messageFiles);
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
                    int destinationPartition = to / config.chunkSize();
                    messageWriters.write(destinationPartition, to, contribution);
                    messageCount++;
                }
                edgeCount++;
            }
        }
        return new ScatterStats(edgeCount, messageCount, 0, List.of());
    }

    private GatherStats gather(
            PreprocessingResult graph,
            Path currentRankPath,
            Path nextRankPath,
            List<MessageFileInfo> messageFiles,
            double base,
            ExecutorService executor
    ) throws IOException {
        Map<Integer, List<MessageFileInfo>> messageIndex = buildMessageBucketIndex(messageFiles);
        MessageBucketLayout bucketLayout = new MessageBucketLayout(
                partitionCount(graph),
                config.chunkSize(),
                graph.vertexCount()
        );
        validateMessageBuckets(messageIndex, bucketLayout);

        List<Callable<GatherStats>> tasks = new ArrayList<>(bucketLayout.bucketCount());
        for (int bucket : bucketsByDescendingMessageBytes(bucketLayout, messageIndex)) {
            List<MessageFileInfo> bucketMessageFiles = messageIndex.getOrDefault(bucket, List.of());
            tasks.add(() -> gatherBucketRange(
                    graph,
                    currentRankPath,
                    nextRankPath,
                    bucket,
                    bucketLayout.firstDenseVertexId(bucket),
                    bucketLayout.endDenseVertexIdExclusive(bucket),
                    bucketMessageFiles,
                    base
            ));
        }
        List<GatherStats> stats = invokeAll(executor, tasks);
        long messages = 0;
        double l1Diff = 0.0;
        double rankSum = 0.0;
        for (GatherStats stat : stats) {
            messages += stat.messageCount();
            l1Diff += stat.l1Diff();
            rankSum += stat.rankSum();
        }
        logger.info("gather messages=%d tasks=%d".formatted(messages, tasks.size()));
        return new GatherStats(messages, l1Diff, rankSum);
    }

    private GatherStats gatherBucketRange(
            PreprocessingResult graph,
            Path currentRankPath,
            Path nextRankPath,
            int bucket,
            long rangeStart,
            long rangeEndExclusive,
            List<MessageFileInfo> messageFiles,
            double base
    ) throws IOException {
        long messageCount = 0;
        double l1Diff = 0.0;
        double rankSum = 0.0;
        int length = Math.toIntExact(rangeEndExclusive - rangeStart);
        double[] values = new double[length];
        Arrays.fill(values, base);

        try (DiskDoubleArray currentRanks = new DiskDoubleArray(currentRankPath, graph.vertexCount(), config.chunkSize(), false);
             DiskDoubleArray nextRanks = new DiskDoubleArray(nextRankPath, graph.vertexCount(), config.chunkSize(), true, false)) {
            for (MessageFileInfo messageFile : messageFiles) {
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
            double[] currentChunk = new double[config.chunkSize()];
            ByteBuffer currentScratch = ByteBuffer.allocate(config.chunkSize() * Double.BYTES);
            for (long start = rangeStart; start < rangeEndExclusive; start += config.chunkSize()) {
                int chunkLength = (int) Math.min(config.chunkSize(), rangeEndExclusive - start);
                int offset = Math.toIntExact(start - rangeStart);
                currentRanks.readChunk(start, currentChunk, 0, chunkLength, currentScratch);
                for (int i = 0; i < chunkLength; i++) {
                    double nextValue = values[offset + i];
                    if (!Double.isFinite(nextValue) || nextValue < 0.0) {
                        throw new IOException("invalid rank value at dense vertex %d: %.12g"
                                .formatted(start + i, nextValue));
                    }
                    l1Diff += Math.abs(nextValue - currentChunk[i]);
                    rankSum += nextValue;
                }
            }
            nextRanks.writeChunk(rangeStart, values, length);
        }
        return new GatherStats(messageCount, l1Diff, rankSum);
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
        validateSourceSliceBudget(graph);
        List<SourcePartitionSlice> slices = new ArrayList<>();
        int previousPartition = -1;
        for (var info : graph.sourcePartitions()) {
            previousPartition = readSourcePartitionInfo(graph, slices, info, previousPartition);
        }
        return slices;
    }

    private int readSourcePartitionInfo(
            PreprocessingResult graph,
            List<SourcePartitionSlice> slices,
            org.example.largegraph.graph.GraphPreprocessor.SourcePartitionInfo info,
            int previousPartition
    ) throws IOException {
        int partition = info.partition();
        long bytes = info.bytes();
        if (partition <= previousPartition) {
            throw new IOException("source partition metadata out of order: previous=%d actual=%d"
                    .formatted(previousPartition, partition));
        }
        if (partition < 0 || partition >= partitionCount(graph)) {
            throw new IOException("source partition metadata out of range: " + partition);
        }
        if (bytes % EDGE_RECORD_BYTES != 0) {
            throw new IOException("source partition byte size is not edge-record aligned: partition=%d bytes=%d"
                    .formatted(partition, bytes));
        }
        long edgeCount = bytes / EDGE_RECORD_BYTES;
        Path partitionPath = graph.edgesDir().resolve("src-part-%05d.bin".formatted(partition));
        long actualBytes = Files.size(partitionPath);
        if (actualBytes != bytes) {
            throw new IOException("source partition file size mismatch: partition=%d metadataBytes=%d actualBytes=%d"
                    .formatted(partition, bytes, actualBytes));
        }
        if (bytes > 0) {
            addSourcePartitionSlices(slices, partition, bytes);
        }
        return partition;
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

    private void validateSourceSliceBudget(PreprocessingResult graph) throws IOException {
        long sliceCount = 0;
        for (var info : graph.sourcePartitions()) {
            long alignedSliceBytes = config.scatterSliceBytes() - (config.scatterSliceBytes() % EDGE_RECORD_BYTES);
            if (alignedSliceBytes == 0) {
                alignedSliceBytes = EDGE_RECORD_BYTES;
            }
            sliceCount = safeAdd(sliceCount, Math.ceilDiv(info.bytes(), alignedSliceBytes));
        }
        long estimatedMetadataBytes = safeMultiply(sliceCount, 56L);
        long metadataBudget = Math.max(1L, MemoryUtils.maxHeapBytes() / 10L);
        if (estimatedMetadataBytes > metadataBudget) {
            throw new IOException("too many source slices for heap; increase --scatter-slice-mb or --chunk-size: slices=%d estimatedMetadata=%s budget=%s"
                    .formatted(
                            sliceCount,
                            MemoryUtils.humanReadableBytes(estimatedMetadataBytes),
                            MemoryUtils.humanReadableBytes(metadataBudget)
                    ));
        }
    }

    private List<Integer> bucketsByDescendingMessageBytes(
            MessageBucketLayout bucketLayout,
            Map<Integer, List<MessageFileInfo>> messageIndex
    ) {
        List<Integer> buckets = new ArrayList<>(bucketLayout.bucketCount());
        for (int bucket = 0; bucket < bucketLayout.bucketCount(); bucket++) {
            buckets.add(bucket);
        }
        buckets.sort(Comparator
                .comparingLong((Integer bucket) -> messageIndex
                        .getOrDefault(bucket, List.of())
                        .stream()
                        .mapToLong(MessageFileInfo::bytes)
                        .sum())
                .reversed()
                .thenComparingInt(Integer::intValue));
        return buckets;
    }

    private Map<Integer, List<MessageFileInfo>> buildMessageBucketIndex(List<MessageFileInfo> messageFiles) throws IOException {
        Map<Integer, List<MessageFileInfo>> index = new HashMap<>();
        for (MessageFileInfo messageFile : messageFiles) {
            if (messageFile.bytes() % (Integer.BYTES + Double.BYTES) != 0) {
                throw new IOException("message file is not record-aligned: " + messageFile.path());
            }
            index.computeIfAbsent(messageFile.bucket(), ignored -> new ArrayList<>()).add(messageFile);
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
            Map<Integer, List<MessageFileInfo>> messageIndex,
            MessageBucketLayout bucketLayout
    ) throws IOException {
        for (int bucket : messageIndex.keySet()) {
            if (bucket < 0 || bucket >= bucketLayout.bucketCount()) {
                throw new IOException("message index references invalid bucket: " + bucket);
            }
        }
    }

    private void validateGatherStats(GatherStats stats) throws IOException {
        if (!Double.isFinite(stats.l1Diff()) || !Double.isFinite(stats.rankSum())) {
            throw new IOException("non-finite convergence stats: diff=%s rankSum=%s"
                    .formatted(stats.l1Diff(), stats.rankSum()));
        }
        if (Math.abs(stats.rankSum() - 1.0) > RANK_SUM_WARNING_THRESHOLD) {
            throw new IOException("rankSum invariant failed: %.12g".formatted(stats.rankSum()));
        }
    }

    private List<ChunkRange> chunkRanges(PreprocessingResult graph, int workerCount) {
        long chunkCount = Math.ceilDiv(graph.vertexCount(), config.chunkSize());
        int rangeCount = Math.toIntExact(Math.min((long) workerCount, Math.max(1L, chunkCount)));
        List<ChunkRange> ranges = new ArrayList<>(rangeCount);
        long chunksPerRange = Math.ceilDiv(chunkCount, rangeCount);
        for (int range = 0; range < rangeCount; range++) {
            long startChunk = range * chunksPerRange;
            long endChunk = Math.min(chunkCount, startChunk + chunksPerRange);
            if (startChunk < endChunk) {
                ranges.add(new ChunkRange(
                        startChunk * (long) config.chunkSize(),
                        Math.min(graph.vertexCount(), endChunk * (long) config.chunkSize())
                ));
            }
        }
        return ranges;
    }

    private int chunkLength(PreprocessingResult graph, long start) {
        return (int) Math.min(config.chunkSize(), graph.vertexCount() - start);
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

    private void cleanupMessages(Path path, IOException primaryFailure) throws IOException {
        try {
            deleteRecursively(path);
        } catch (IOException cleanupFailure) {
            if (primaryFailure != null) {
                primaryFailure.addSuppressed(cleanupFailure);
            } else {
                throw cleanupFailure;
            }
        }
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
            double rankSum
    ) {
    }

    private record ScatterStats(long edgeCount, long messageCount, long messageBytes, List<MessageFileInfo> messageFiles) {
    }

    private record GatherStats(long messageCount, double l1Diff, double rankSum) {
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
