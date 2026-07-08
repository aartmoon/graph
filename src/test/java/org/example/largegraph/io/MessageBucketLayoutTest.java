package org.example.largegraph.io;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class MessageBucketLayoutTest {
    @Test
    void assignsContiguousDestinationPartitionRangesToBuckets() {
        MessageBucketLayout layout = new MessageBucketLayout(20_000);

        assertEquals(6_667, layout.bucketCount());
        assertEquals(0, layout.bucketFor(0));
        assertEquals(0, layout.bucketFor(2));
        assertEquals(1, layout.bucketFor(3));
        assertEquals(5_000, layout.bucketFor(15_000));
        assertEquals(layout.bucketCount() - 1, layout.bucketFor(19_999));
    }

    @Test
    void reportsRangeCoveredByBucket() {
        MessageBucketLayout layout = new MessageBucketLayout(10);

        assertEquals(3, layout.firstPartition(3));
        assertEquals(4, layout.endPartitionExclusive(3));
    }
}
