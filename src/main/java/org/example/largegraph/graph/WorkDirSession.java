package org.example.largegraph.graph;

import org.example.largegraph.util.FileUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class WorkDirSession implements Closeable {
    private static final String WORKDIR_MARKER = ".largegraph-workdir";

    private final Path workDir;
    private final FileChannel lockChannel;
    private final FileLock lock;

    private WorkDirSession(Path workDir, FileChannel lockChannel, FileLock lock) {
        this.workDir = workDir;
        this.lockChannel = lockChannel;
        this.lock = lock;
    }

    public static WorkDirSession acquire(Path workDir) throws IOException {
        Files.createDirectories(workDir);
        if (!Files.isDirectory(workDir)) {
            throw new IOException("workdir path is not a directory: " + workDir);
        }
        Path lockPath = workDir.resolve(".lock");
        FileChannel channel = FileChannel.open(
                lockPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        FileLock fileLock;
        try {
            fileLock = channel.tryLock();
        } catch (OverlappingFileLockException ex) {
            channel.close();
            throw new IOException("workdir is already locked by this process: " + workDir, ex);
        }
        if (fileLock == null) {
            channel.close();
            throw new IOException("workdir is already locked by another process: " + workDir);
        }
        return new WorkDirSession(workDir, channel, fileLock);
    }

    public void prepare() throws IOException {
        Path marker = workDir.resolve(WORKDIR_MARKER);
        try (var entries = Files.list(workDir)) {
            boolean hasNonLockEntry = entries
                    .anyMatch(path -> !".lock".equals(path.getFileName().toString()));
            if (hasNonLockEntry && !Files.exists(marker)) {
                throw new IOException("refusing to clean unmarked workdir: " + workDir);
            }
        }
        Files.writeString(marker, "largegraph workdir\n", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        cleanupManagedPaths();
        Files.createDirectories(workDir.resolve("edges_by_source"));
    }

    public void cleanupAfterSuccess() throws IOException {
        cleanupManagedPaths();
        Files.deleteIfExists(workDir.resolve(WORKDIR_MARKER));
    }

    private void cleanupManagedPaths() throws IOException {
        FileUtils.deleteRecursively(workDir.resolve("vertex"));
        FileUtils.deleteRecursively(workDir.resolve("edges_by_source"));
        FileUtils.deleteRecursively(workDir.resolve("messages"));
        FileUtils.deleteRecursively(workDir.resolve("sort"));
        Files.deleteIfExists(workDir.resolve("vertices.bin"));
        Files.deleteIfExists(workDir.resolve("endpoint_refs.bin"));
        Files.deleteIfExists(workDir.resolve("endpoint_refs.sorted.bin"));
        Files.deleteIfExists(workDir.resolve("endpoint_assignments.bin"));
        Files.deleteIfExists(workDir.resolve("endpoint_assignments.sorted.bin"));
        Files.deleteIfExists(workDir.resolve("dense_edges.bin"));
    }

    @Override
    public void close() throws IOException {
        try {
            lock.release();
        } finally {
            lockChannel.close();
        }
    }
}
