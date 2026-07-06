package org.example.largegraph.graph;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class OutDegreeStore {
    private final Path path;

    public OutDegreeStore(Path workDir) {
        this.path = workDir.resolve("out_degree.bin");
    }

    public void write(int[] outDegree) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            for (int value : outDegree) {
                output.writeInt(value);
            }
        }
    }

    public int[] read(int vertexCount) throws IOException {
        int[] outDegree = new int[vertexCount];
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            for (int i = 0; i < vertexCount; i++) {
                outDegree[i] = input.readInt();
            }
        }
        return outDegree;
    }

    public Path path() {
        return path;
    }
}
