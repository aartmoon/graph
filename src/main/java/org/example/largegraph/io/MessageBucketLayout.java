package org.example.largegraph.io;

public final class MessageBucketLayout {
    public static final int MAX_BUCKETS = 8_192;

    private final int destinationPartitionCount;
    private final long vertexCount;
    private final int bucketCount;
    private final long verticesPerBucket;
    private final int chunkSize;

    public MessageBucketLayout(int destinationPartitionCount, int chunkSize, long vertexCount) {
        if (destinationPartitionCount <= 0) {
            throw new IllegalArgumentException("destinationPartitionCount must be positive");
        }
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be positive");
        }
        if (vertexCount <= 0) {
            throw new IllegalArgumentException("vertexCount must be positive");
        }
        this.destinationPartitionCount = destinationPartitionCount;
        this.chunkSize = chunkSize;
        this.vertexCount = vertexCount;
        int targetBucketCount = Math.toIntExact(Math.min(vertexCount, (long) MAX_BUCKETS));
        this.verticesPerBucket = Math.ceilDiv(vertexCount, targetBucketCount);
        this.bucketCount = Math.toIntExact(Math.ceilDiv(vertexCount, verticesPerBucket));
    }

    public int bucketFor(int destinationPartition, int destinationVertexDenseId) {
        if (destinationPartition < 0 || destinationPartition >= destinationPartitionCount) {
            throw new IllegalArgumentException("invalid destination partition: " + destinationPartition);
        }
        if (destinationVertexDenseId < 0 || destinationVertexDenseId >= vertexCount) {
            throw new IllegalArgumentException("invalid destination vertex id: " + destinationVertexDenseId);
        }
        int actualPartition = destinationVertexDenseId / chunkSize;
        if (actualPartition != destinationPartition) {
            throw new IllegalArgumentException("destination vertex does not belong to partition: vertex=%d partition=%d"
                    .formatted(destinationVertexDenseId, destinationPartition));
        }
        return Math.toIntExact(destinationVertexDenseId / verticesPerBucket);
    }

    public int firstPartition(int bucket) {
        validateBucket(bucket);
        return Math.toIntExact(firstDenseVertexId(bucket) / chunkSize);
    }

    public int endPartitionExclusive(int bucket) {
        validateBucket(bucket);
        long lastDenseIdExclusive = endDenseVertexIdExclusive(bucket);
        return Math.min(destinationPartitionCount, Math.toIntExact(Math.ceilDiv(lastDenseIdExclusive, chunkSize)));
    }

    public long firstDenseVertexId(int bucket) {
        validateBucket(bucket);
        return bucket * verticesPerBucket;
    }

    public long endDenseVertexIdExclusive(int bucket) {
        validateBucket(bucket);
        return Math.min(vertexCount, (bucket + 1) * verticesPerBucket);
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
