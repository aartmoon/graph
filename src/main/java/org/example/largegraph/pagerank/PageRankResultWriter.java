package org.example.largegraph.pagerank;

import org.example.largegraph.config.AppConfig;
import org.example.largegraph.pagerank.PageRankEngine.PageRankRunResult;
import org.example.largegraph.storage.DiskDoubleArray;
import org.example.largegraph.storage.DiskIntArray;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

public final class PageRankResultWriter {
    private final AppConfig config;

    public PageRankResultWriter(AppConfig config) {
        this.config = config;
    }

    public void write(PageRankRunResult result) throws IOException {
        if (config.output().getParent() != null) {
            Files.createDirectories(config.output().getParent());
        }
        checkOutputDiskSpace(result);
        Path tempOutput = temporarySibling(config.output());
        boolean moved = false;
        try {
            writeAllStreaming(result, tempOutput);
            moveReplacing(tempOutput, config.output());
            moved = true;
        } finally {
            if (!moved) {
                Files.deleteIfExists(tempOutput);
            }
        }
    }

    private void writeAllStreaming(PageRankRunResult result, Path output) throws IOException {
        try (DiskDoubleArray ranks = new DiskDoubleArray(result.rankPath(), result.vertexCount(), config.chunkSize(), false);
             DiskIntArray originalIds = new DiskIntArray(result.verticesPath(), result.vertexCount(), config.chunkSize(), false);
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("vertex,rank");
            writer.newLine();
            double[] rankChunk = new double[config.chunkSize()];
            int[] originalIdChunk = new int[config.chunkSize()];
            ByteBuffer rankScratch = ByteBuffer.allocate(config.chunkSize() * Double.BYTES);
            ByteBuffer idScratch = ByteBuffer.allocate(config.chunkSize() * Integer.BYTES);
            for (long start = 0; start < result.vertexCount(); start += config.chunkSize()) {
                int length = (int) Math.min(config.chunkSize(), result.vertexCount() - start);
                ranks.readChunk(start, rankChunk, 0, length, rankScratch);
                originalIds.readIntChunk(start, originalIdChunk, 0, length, idScratch);
                for (int i = 0; i < length; i++) {
                    writer.write(Integer.toString(originalIdChunk[i]));
                    writer.write(',');
                    writer.write(Double.toString(rankChunk[i]));
                    writer.newLine();
                }
            }
        }
    }

    private Path temporarySibling(Path output) {
        Path fileName = output.getFileName();
        String tempName = fileName + ".tmp-" + UUID.randomUUID();
        Path parent = output.getParent();
        return parent == null ? Path.of(tempName) : parent.resolve(tempName);
    }

    private void checkOutputDiskSpace(PageRankRunResult result) throws IOException {
        Path outputDir = config.output().getParent() == null ? Path.of(".") : config.output().getParent();
        FileStore store = Files.getFileStore(outputDir);
        long estimatedBytes = saturatingAdd(saturatingMultiply(result.vertexCount(), 64L), 1024L * 1024L);
        long free = store.getUsableSpace();
        if (free < estimatedBytes) {
            throw new IOException("not enough disk space for output CSV: free=%d estimatedRequired=%d"
                    .formatted(free, estimatedBytes));
        }
    }

    private void moveReplacing(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ex) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private long saturatingMultiply(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

    private long saturatingAdd(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException ex) {
            return Long.MAX_VALUE;
        }
    }

}
