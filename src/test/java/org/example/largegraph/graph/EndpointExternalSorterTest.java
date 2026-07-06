package org.example.largegraph.graph;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class EndpointExternalSorterTest {
    @TempDir
    Path tempDir;

    @Test
    void endpointRefSorterOrdersPrimitiveRunsAndMergePasses() throws IOException {
        Path input = tempDir.resolve("endpoint_refs.bin");
        Path output = tempDir.resolve("endpoint_refs.sorted.bin");
        List<EndpointRefRecord> records = new ArrayList<>();
        for (int i = 139; i >= 0; i--) {
            records.add(new EndpointRefRecord(i % 11, 1_000L - (i % 17), (byte) (i % 2)));
        }
        records.add(new EndpointRefRecord(3, 7L, (byte) 1));
        records.add(new EndpointRefRecord(3, 7L, (byte) 0));
        records.add(new EndpointRefRecord(3, 6L, (byte) 1));
        writeEndpointRefs(input, records);

        new EndpointRefExternalSorter(1).sort(
                input,
                output,
                tempDir.resolve("sort").resolve("endpoint-refs"),
                "endpoint-ref-run"
        );

        List<EndpointRefRecord> expected = records.stream()
                .sorted(Comparator
                        .comparingInt(EndpointRefRecord::originalId)
                        .thenComparingLong(EndpointRefRecord::edgeId)
                        .thenComparingInt(EndpointRefRecord::side))
                .toList();
        assertEquals(expected, readEndpointRefs(output));
    }

//    @Test
//    void endpointAssignmentSorterOrdersPrimitiveRunsAndMergePasses() throws IOException {
//        Path input = tempDir.resolve("endpoint_assignments.bin");
//        Path output = tempDir.resolve("endpoint_assignments.sorted.bin");
//        List<EndpointAssignmentRecord> records = new ArrayList<>();
//        for (int i = 149; i >= 0; i--) {
//            records.add(new EndpointAssignmentRecord(10_000L - (i % 23), (byte) (i % 2), i));
//        }
//        records.add(new EndpointAssignmentRecord(5L, (byte) 1, 20));
//        records.add(new EndpointAssignmentRecord(5L, (byte) 0, 10));
//        records.add(new EndpointAssignmentRecord(4L, (byte) 1, 30));
//        writeEndpointAssignments(input, records);
//
//        new EndpointAssignmentExternalSorter(1).sort(
//                input,
//                output,
//                tempDir.resolve("sort").resolve("endpoint-assignments"),
//                "endpoint-assignment-run"
//        );
//
//        List<EndpointAssignmentRecord> expected = records.stream()
//                .sorted(Comparator
//                        .comparingLong(EndpointAssignmentRecord::edgeId)
//                        .thenComparingInt(EndpointAssignmentRecord::side))
//                .toList();
//        assertEquals(expected, readEndpointAssignments(output));
//    }

    private static void writeEndpointRefs(Path path, List<EndpointRefRecord> records) throws IOException {
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            for (EndpointRefRecord record : records) {
                output.writeInt(record.originalId());
                output.writeLong(record.edgeId());
                output.writeByte(record.side());
            }
        }
    }

    private static List<EndpointRefRecord> readEndpointRefs(Path path) throws IOException {
        List<EndpointRefRecord> records = new ArrayList<>();
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            while (true) {
                try {
                    records.add(new EndpointRefRecord(input.readInt(), input.readLong(), input.readByte()));
                } catch (EOFException ex) {
                    return records;
                }
            }
        }
    }

    private static void writeEndpointAssignments(Path path, List<EndpointAssignmentRecord> records) throws IOException {
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path)))) {
            for (EndpointAssignmentRecord record : records) {
                output.writeLong(record.edgeId());
                output.writeByte(record.side());
                output.writeInt(record.denseId());
            }
        }
    }

    private static List<EndpointAssignmentRecord> readEndpointAssignments(Path path) throws IOException {
        List<EndpointAssignmentRecord> records = new ArrayList<>();
        try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            while (true) {
                try {
                    records.add(new EndpointAssignmentRecord(input.readLong(), input.readByte(), input.readInt()));
                } catch (EOFException ex) {
                    return records;
                }
            }
        }
    }

    private record EndpointRefRecord(int originalId, long edgeId, byte side) {
    }

    private record EndpointAssignmentRecord(long edgeId, byte side, int denseId) {
    }
}
