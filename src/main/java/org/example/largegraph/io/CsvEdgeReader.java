package org.example.largegraph.io;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class CsvEdgeReader implements Closeable {
    private final BufferedReader reader;
    private long lineNumber;
    private boolean firstDataLine = true;
    private int from;
    private int to;

    public CsvEdgeReader(Path path) throws IOException {
        this.reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
    }

    public boolean next() throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            if (lineNumber == 1 && line.startsWith("\uFEFF")) {
                line = line.substring(1);
            }
            String trimmed = line.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (firstDataLine && isHeader(trimmed)) {
                firstDataLine = false;
                continue;
            }
            firstDataLine = false;
            parseEdge(trimmed);
            return true;
        }
        return false;
    }

    public int from() {
        return from;
    }

    public int to() {
        return to;
    }

    private void parseEdge(String line) {
        int comma = line.indexOf(',');
        if (comma < 0) {
            throw new IllegalArgumentException("invalid CSV edge at line " + lineNumber
                    + ": expected at least two columns: " + line);
        }
        int secondComma = line.indexOf(',', comma + 1);
        int toEnd = secondComma >= 0 ? secondComma : line.length();
        String fromText = line.substring(0, comma).trim();
        String toText = line.substring(comma + 1, toEnd).trim();
        if (fromText.isEmpty() || toText.isEmpty()) {
            throw new IllegalArgumentException("empty vertex id at line " + lineNumber + ": " + line);
        }
        try {
            from = Integer.parseInt(fromText);
            to = Integer.parseInt(toText);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("invalid int32 vertex id at line " + lineNumber + ": " + line, ex);
        }
    }

    private boolean isHeader(String line) {
        int comma = line.indexOf(',');
        if (comma < 0) {
            return false;
        }
        int secondComma = line.indexOf(',', comma + 1);
        int toEnd = secondComma >= 0 ? secondComma : line.length();
        return "from".equalsIgnoreCase(line.substring(0, comma).trim())
                && "to".equalsIgnoreCase(line.substring(comma + 1, toEnd).trim());
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

}
