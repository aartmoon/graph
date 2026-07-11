package org.example.largegraph.io;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class BinaryEdgeWriter implements Closeable {
    private final DataOutputStream output;

    public BinaryEdgeWriter(Path path) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        )));
    }

    public void write(int denseFrom, int denseTo) throws IOException {
        output.writeInt(denseFrom);
        output.writeInt(denseTo);
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
