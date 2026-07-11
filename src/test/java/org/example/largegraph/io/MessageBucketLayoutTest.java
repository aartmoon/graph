package org.example.largegraph.io;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class MessageBucketLayoutTest {
    @Test
    void assignsContiguousDenseVertexRangesToBuckets() {
        MessageBucketLayout layout = new MessageBucketLayout(17, 1_000, 16_384);

        assertEquals(17, layout.bucketCount());
        assertEquals(0, layout.bucketFor(0));
        assertEquals(0, layout.bucketFor(999));
        assertEquals(1, layout.bucketFor(1_000));
        assertEquals(15, layout.bucketFor(15_000));
        assertEquals(layout.bucketCount() - 1, layout.bucketFor(16_383));
    }

    @Test
    void reportsRangesCoveredByBucket() {
        MessageBucketLayout layout = new MessageBucketLayout(10_000, 10, 100_000);

        assertEquals(39, layout.firstDenseVertexId(3));
        assertEquals(52, layout.endDenseVertexIdExclusive(3));
    }
}
