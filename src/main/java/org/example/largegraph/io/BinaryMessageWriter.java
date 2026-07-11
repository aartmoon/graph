package org.example.largegraph.io;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class BinaryMessageWriter implements Closeable {
    private final DataOutputStream output;

    public BinaryMessageWriter(Path path) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )));
    }

    public void write(int to, double contribution) throws IOException {
        output.writeInt(to);
        output.writeDouble(contribution);
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
