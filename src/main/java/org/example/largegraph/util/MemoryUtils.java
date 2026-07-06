package org.example.largegraph.util;

public final class MemoryUtils {
    private MemoryUtils() {
    }

    public static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    public static long maxHeapBytes() {
        return Runtime.getRuntime().maxMemory();
    }

    public static String humanReadableBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        double value = bytes;
        String[] units = {"KB", "MB", "GB", "TB"};
        int unit = -1;
        do {
            value /= 1024.0;
            unit++;
        } while (value >= 1024.0 && unit < units.length - 1);
        return "%.2f %s".formatted(value, units[unit]);
    }
}
