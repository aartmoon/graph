package org.example.largegraph.io;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public final class BinaryMessageReader implements Closeable {
    private final DataInputStream input;

    public BinaryMessageReader(Path path) throws IOException {
        this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
    }

    public Optional<Message> next() throws IOException {
        try {
            int to = input.readInt();
            double contribution = input.readDouble();
            return Optional.of(new Message(to, contribution));
        } catch (EOFException ex) {
            return Optional.empty();
        }
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    public record Message(int to, double contribution) {
    }
}
