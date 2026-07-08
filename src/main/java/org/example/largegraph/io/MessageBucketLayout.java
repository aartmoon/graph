package org.example.largegraph.io;

public final class MessageBucketLayout {
    public static final int MAX_BUCKETS = 8_192;

    private final int destinationPartitionCount;
    private final int bucketCount;
    private final int partitionsPerBucket;

    public MessageBucketLayout(int destinationPartitionCount) {
        if (destinationPartitionCount <= 0) {
            throw new IllegalArgumentException("destinationPartitionCount must be positive");
        }
        this.destinationPartitionCount = destinationPartitionCount;
        int targetBucketCount = Math.min(destinationPartitionCount, MAX_BUCKETS);
        this.partitionsPerBucket = Math.ceilDiv(destinationPartitionCount, targetBucketCount);
        this.bucketCount = Math.ceilDiv(destinationPartitionCount, partitionsPerBucket);
    }

    public int bucketFor(int destinationPartition) {
        if (destinationPartition < 0 || destinationPartition >= destinationPartitionCount) {
            throw new IllegalArgumentException("invalid destination partition: " + destinationPartition);
        }
        return destinationPartition / partitionsPerBucket;
    }

    public int firstPartition(int bucket) {
        validateBucket(bucket);
        return bucket * partitionsPerBucket;
    }

    public int endPartitionExclusive(int bucket) {
        validateBucket(bucket);
        return Math.min(destinationPartitionCount, (bucket + 1) * partitionsPerBucket);
    }

    public int bucketCount() {
        return bucketCount;
    }

    private void validateBucket(int bucket) {
        if (bucket < 0 || bucket >= bucketCount) {
            throw new IllegalArgumentException("invalid message bucket: " + bucket);
        }
    }
}
