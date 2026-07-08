package org.example.largegraph.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;

final class BinaryReaderValidationTest {
    @TempDir
    Path tempDir;

    @Test
    void messageReaderRejectsPartialRecord() throws IOException {
        Path path = tempDir.resolve("messages.bin");
        Files.write(path, new byte[Integer.BYTES + 1]);

        assertThrows(IOException.class, () -> new BinaryMessageReader(path));
    }

    @Test
    void edgeReaderRejectsPartialRecord() throws IOException {
        Path path = tempDir.resolve("edges.bin");
        Files.write(path, new byte[Integer.BYTES * 2 - 1]);

        assertThrows(IOException.class, () -> new BinaryEdgeReader(path));
    }
}
