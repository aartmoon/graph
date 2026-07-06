package org.example.largegraph.util;

import java.time.Instant;

public final class ProgressLogger {
    public void info(String message) {
        System.out.printf("%s INFO  %s%n", Instant.now(), message);
    }
}
