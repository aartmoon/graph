package org.example.largegraph.io;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public final class BinaryEdgeReader implements Closeable {
    private final DataInputStream input;

    public BinaryEdgeReader(Path path) throws IOException {
        this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
    }

    public Optional<DenseEdge> next() throws IOException {
        try {
            int denseFrom = input.readInt();
            int denseTo = input.readInt();
            return Optional.of(new DenseEdge(denseFrom, denseTo));
        } catch (EOFException ex) {
            return Optional.empty();
        }
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    public record DenseEdge(int denseFrom, int denseTo) {
    }
}
