package org.example.largegraph.io;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class BinaryEdgeWriter implements Closeable {
    private final DataOutputStream output;
    private long edgeCount;

    public BinaryEdgeWriter(Path path) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)));
    }

    public void write(int denseFrom, int denseTo) throws IOException {
        output.writeInt(denseFrom);
        output.writeInt(denseTo);
        edgeCount++;
    }

    public long edgeCount() {
        return edgeCount;
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
