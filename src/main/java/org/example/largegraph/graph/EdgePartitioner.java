package org.example.largegraph.graph;

import org.example.largegraph.io.BinaryEdgeWriter;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class EdgePartitioner implements AutoCloseable {
    private final Path partitionsDir;
    private final int partitionCount;
    private final BinaryEdgeWriter[] writers;

    public EdgePartitioner(Path workDir, int partitionCount) throws IOException {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }
        this.partitionsDir = workDir.resolve("partitions");
        this.partitionCount = partitionCount;
        this.writers = new BinaryEdgeWriter[partitionCount];
        Files.createDirectories(partitionsDir);
        createEmptyPartitionFiles();
    }

    public void write(int denseFrom, int denseTo) throws IOException {
        writerFor(partitionId(denseTo)).write(denseFrom, denseTo);
    }

    public int partitionId(int denseTo) {
        return Math.floorMod(denseTo, partitionCount);
    }

    public Path partitionPath(int partitionId) {
        return partitionsDir.resolve("part-%05d.bin".formatted(partitionId));
    }

    public int partitionCount() {
        return partitionCount;
    }

    private BinaryEdgeWriter writerFor(int partitionId) throws IOException {
        BinaryEdgeWriter writer = writers[partitionId];
        if (writer == null) {
            writer = new BinaryEdgeWriter(partitionPath(partitionId));
            writers[partitionId] = writer;
        }
        return writer;
    }

    private void createEmptyPartitionFiles() throws IOException {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Files.deleteIfExists(partitionPath(partitionId));
            Files.createFile(partitionPath(partitionId));
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (BinaryEdgeWriter writer : writers) {
            if (writer == null) {
                continue;
            }
            try {
                writer.close();
            } catch (IOException ex) {
                if (failure == null) {
                    failure = ex;
                } else {
                    failure.addSuppressed(ex);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
