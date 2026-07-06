package org.example.largegraph.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class VertexIndexer {
    private final Map<Integer, Integer> originalToDense = new HashMap<>();
    private final List<Integer> denseToOriginal = new ArrayList<>();

    public int getOrCreateDenseId(int originalId) {
        Integer existing = originalToDense.get(originalId);
        if (existing != null) {
            return existing;
        }
        int denseId = denseToOriginal.size();
        originalToDense.put(originalId, denseId);
        denseToOriginal.add(originalId);
        return denseId;
    }

    public int denseId(int originalId) {
        Integer denseId = originalToDense.get(originalId);
        if (denseId == null) {
            throw new IllegalArgumentException("unknown vertex id: " + originalId);
        }
        return denseId;
    }

    public int originalId(int denseId) {
        return denseToOriginal.get(denseId);
    }

    public int size() {
        return denseToOriginal.size();
    }
}
