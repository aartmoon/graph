package org.example.largegraph.io;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BinaryMessageReader implements Closeable {
    private static final int RECORD_BYTES = Integer.BYTES + Double.BYTES;

    private final DataInputStream input;
    private int to;
    private double contribution;

    public BinaryMessageReader(Path path) throws IOException {
        long bytes = Files.size(path);
        if (bytes % RECORD_BYTES != 0) {
            throw new IOException("corrupted message file size: %s bytes=%d recordBytes=%d"
                    .formatted(path, bytes, RECORD_BYTES));
        }
        this.input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)));
    }

    public boolean next() throws IOException {
        try {
            to = input.readInt();
            contribution = input.readDouble();
            return true;
        } catch (EOFException ex) {
            return false;
        }
    }

    public int to() {
        return to;
    }

    public double contribution() {
        return contribution;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
