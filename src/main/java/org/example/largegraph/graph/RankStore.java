package org.example.largegraph.graph;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class RankStore {
    private final Path workDir;

    public RankStore(Path workDir) {
        this.workDir = workDir;
    }

    public Path currentRankPath() {
        return workDir.resolve("current_rank.bin");
    }

    public Path nextRankPath() {
        return workDir.resolve("next_rank.bin");
    }

    public void initializeUniform(int vertexCount) throws IOException {
        Files.createDirectories(workDir);
        double initialRank = vertexCount == 0 ? 0.0 : 1.0 / vertexCount;
        writeAll(currentRankPath(), vertexCount, initialRank);
        writeAll(nextRankPath(), vertexCount, 0.0);
    }

    public double[] readCurrent(int vertexCount) throws IOException {
        return readAll(currentRankPath(), vertexCount);
    }

    public void writeCurrent(double[] ranks) throws IOException {
        writeAll(currentRankPath(), ranks);
    }

    public void writeNext(double[] ranks) throws IOException {
        writeAll(nextRankPath(), ranks);
    }

    private void writeAll(Path path, int count, double value) throws IOException {
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            for (int i = 0; i < count; i++) {
                output.writeDouble(value);
            }
        }
    }

    private void writeAll(Path path, double[] ranks) throws IOException {
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            for (double rank : ranks) {
                output.writeDouble(rank);
            }
        }
    }

    private double[] readAll(Path path, int count) throws IOException {
        double[] ranks = new double[count];
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            for (int i = 0; i < count; i++) {
                ranks[i] = input.readDouble();
            }
        }
        return ranks;
    }
}
