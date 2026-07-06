package org.example.largegraph.graph;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class VertexIdStore {
    private final Path path;

    public VertexIdStore(Path workDir) {
        this.path = workDir.resolve("vertex_ids.bin");
    }

    public void write(VertexIndexer indexer) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            for (int denseId = 0; denseId < indexer.size(); denseId++) {
                output.writeInt(indexer.originalId(denseId));
            }
        }
    }

    public int[] read(int vertexCount) throws IOException {
        int[] vertexIds = new int[vertexCount];
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            for (int denseId = 0; denseId < vertexCount; denseId++) {
                vertexIds[denseId] = input.readInt();
            }
        }
        return vertexIds;
    }

    public Path path() {
        return path;
    }
}
