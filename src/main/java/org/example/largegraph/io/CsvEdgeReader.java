package org.example.largegraph.io;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public final class CsvEdgeReader implements Closeable {
    private final BufferedReader reader;
    private long lineNumber;
    private boolean firstDataLine = true;

    public CsvEdgeReader(Path path) throws IOException {
        this.reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
    }

    public Optional<Edge> next() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (firstDataLine && isHeader(trimmed)) {
                firstDataLine = false;
                continue;
            }
            firstDataLine = false;
            return Optional.of(parseEdge(trimmed));
        }
        return Optional.empty();
    }

    private Edge parseEdge(String line) {
        String[] parts = line.split(",", -1);
        if (parts.length != 2) {
            throw new IllegalArgumentException("invalid CSV edge at line " + lineNumber + ": " + line);
        }
        if (parts[0].isBlank() || parts[1].isBlank()) {
            throw new IllegalArgumentException("empty vertex id at line " + lineNumber + ": " + line);
        }
        try {
            return new Edge(Integer.parseInt(parts[0].trim()), Integer.parseInt(parts[1].trim()));
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("invalid int32 vertex id at line " + lineNumber + ": " + line, ex);
        }
    }

    private boolean isHeader(String line) {
        return "from,to".equalsIgnoreCase(line.replace(" ", ""));
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public record Edge(int from, int to) {
    }
}
