package org.xenei.rdfstore;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.SetOperations;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;

public class FixedSizeGatedList<T> {
    private int maxCount;
    private BloomFilter gatekeeper;
    private List<T> list;

    public FixedSizeGatedList(Shape shape, int maxCount) {
        this.maxCount = maxCount;
        gatekeeper = new SimpleBloomFilter(shape);
        list = new ArrayList<>();
    }

    public static Hasher createHasher(Object item) {
        int hashCode = item.hashCode();
        return new EnhancedDoubleHasher((hashCode & 0xFFFF0000), (hashCode & 0xFFFF));
    }

    private long toLong(final long part1, final int part2) {
        return (part1 << Integer.SIZE) | part2;
    }

    public GatedList.Result register(BloomFilter filter, T item) {
        if (gatekeeper.contains(filter)) {
            for (int i = 0; i < list.size(); i++) {
                if (item.equals(list.get(i))) {
                    return new GatedList.Result(true, toLong(item.hashCode(), i));
                }
            }
        }
        if (list.size() < this.maxCount) {
            // add the entry
            gatekeeper.merge(filter);
            list.add(item);
            return new GatedList.Result(false, toLong(item.hashCode(), list.size() - 1));
        }
        return new GatedList.Result(false, -1);
    }

    public void remove(int space) {
        list.set(space, null);
    }

    public T get(int space) {
        return list.get(space);
    }

    public GatedList.Result put(int space, BloomFilter filter, T item) {
        list.set(space, item);
        gatekeeper.merge(filter);
        if (gatekeeper.estimateN() > (this.maxCount * 1.1)) {
            recalcGatekeeper();
        }
        return new GatedList.Result(false, space);
    }

    public GatedList.Result contains(BloomFilter filter, T item) {
        if (gatekeeper.contains(filter)) {
            for (int i = 0; i < list.size(); i++) {
                if (item.equals(list.get(i))) {
                    return new GatedList.Result(true, toLong(item.hashCode(), i));
                }
            }
        }
        return new GatedList.Result(false, -1);
    }

    public int size() {
        return list.size();
    }

    public boolean hasSpace() {
        return list.size() < this.maxCount;
    }

    public int distance(BloomFilter filter) {
        return SetOperations.hammingDistance(filter, gatekeeper);
    }

    private void recalcGatekeeper() {
        BloomFilter newGatekeeper = new SimpleBloomFilter(gatekeeper.getShape());
        for (T t : list) {
            if (t != null) {
                newGatekeeper.merge(createHasher(t));
            }
        }
        gatekeeper = newGatekeeper;
    }
}