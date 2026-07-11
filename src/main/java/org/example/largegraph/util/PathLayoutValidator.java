package org.example.largegraph.util;

import org.example.largegraph.config.AppConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class PathLayoutValidator {
    private PathLayoutValidator() {
    }

    public static void validate(AppConfig config) throws IOException {
        if (!Files.exists(config.input())) {
            throw new IOException("input file does not exist: " + config.input());
        }
        if (!Files.isRegularFile(config.input())) {
            throw new IOException("input path is not a regular file: " + config.input());
        }
        Path input = config.input().toRealPath();
        Path work = resolvePossiblyMissing(config.workDir());
        Path output = resolvePossiblyMissing(config.output());
        if (input.equals(output)) throw new IOException("input and output must be different files");
        if (input.startsWith(work)) throw new IOException("input must not be located inside workdir");
        if (output.startsWith(work)) throw new IOException("output must not be located inside workdir");
    }

    private static Path resolvePossiblyMissing(Path path) throws IOException {
        if (Files.exists(path)) return path.toRealPath();
        Path absolute = path.toAbsolutePath().normalize();
        Path parent = absolute.getParent();
        while (parent != null && !Files.exists(parent)) parent = parent.getParent();
        if (parent == null) return absolute;
        return parent.toRealPath().resolve(parent.relativize(absolute)).normalize();
    }
}
