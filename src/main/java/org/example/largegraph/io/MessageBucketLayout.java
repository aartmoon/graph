package org.example.largegraph.io;

public final class MessageBucketLayout {
    public static final int MAX_BUCKETS = 8_192;

    private final long vertexCount;
    private final int bucketCount;
    private final long verticesPerBucket;

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
        this.vertexCount = vertexCount;
        long calculatedVerticesPerBucket = Math.ceilDiv(vertexCount, MAX_BUCKETS);
        this.verticesPerBucket = Math.max(chunkSize, calculatedVerticesPerBucket);
        this.bucketCount = Math.toIntExact(Math.ceilDiv(vertexCount, verticesPerBucket));
    }

    public int bucketFor(int destinationVertexDenseId) {
        if (destinationVertexDenseId < 0 || destinationVertexDenseId >= vertexCount) {
            throw new IllegalArgumentException("invalid destination vertex id: " + destinationVertexDenseId);
        }
        return Math.toIntExact(destinationVertexDenseId / verticesPerBucket);
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

    public long verticesPerBucket() {
        return verticesPerBucket;
    }

    private void validateBucket(int bucket) {
        if (bucket < 0 || bucket >= bucketCount) {
            throw new IllegalArgumentException("invalid message bucket: " + bucket);
        }
    }
}
