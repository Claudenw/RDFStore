package org.xenei.rdfstore;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;

public class GatedList<T> {
    int maxCount;
    BloomFilter gatekeeper;
    List<T> list;

    public static class Result {
        public final boolean existed;
        public final long value;

        public Result(boolean existed, long value) {
            this.existed = existed;
            this.value = value;
        }
    }

    public GatedList() {
        maxCount = 1000;
        gatekeeper = new SimpleBloomFilter(Shape.fromNP(maxCount, 1.0 / maxCount));
        list = new ArrayList<>();
    }

    private long toLong(final long part1, final int part2) {
        return (part1 << Integer.SIZE) | part2;
    }

    public Result register(T item) {
        int hashCode = item.hashCode();
        Hasher hasher = new EnhancedDoubleHasher((hashCode & 0xFFFF0000), (hashCode & 0xFFFF));
        if (gatekeeper.contains(hasher)) {
            for (int i = 0; i < list.size(); i++) {
                if (item.equals(list.get(i))) {
                    return new Result(true, toLong(hashCode, i));
                }
            }
        } // add the entry
        gatekeeper.merge(hasher);
        list.add(item);
        if (list.size() > maxCount) {
            maxCount += 1000;
            recalcGatekeeper();
        }
        return new Result(false, toLong(hashCode, list.size() - 1));
    }

    public boolean contains(T item) {
        int hashCode = item.hashCode();
        Hasher hasher = new EnhancedDoubleHasher((hashCode & 0xFFFF0000), (hashCode & 0xFFFF));
        return gatekeeper.contains(hasher);
    }

    public int size() {
        return list.size();
    }

    private void recalcGatekeeper() {
        Double probability = 1.0 / maxCount;
        if (probability < Double.MIN_VALUE) {
            probability = Double.MIN_VALUE;
        }
        BloomFilter newGatekeeper = new SimpleBloomFilter(Shape.fromNP(maxCount, probability));
        for (T t : list) {
            int hashCode = t.hashCode();
            Hasher hasher = new EnhancedDoubleHasher((hashCode & 0xFFFF0000), (hashCode & 0xFFFF));
            newGatekeeper.merge(hasher);
        }
        gatekeeper = newGatekeeper;
    }
}