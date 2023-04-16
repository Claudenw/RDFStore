package org.xenei.rdfstore;

import java.util.Iterator;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.xenei.rdfstore.Store.Result;

public class StringPage<T> implements Store.Page<T, Store.Result> {

    private final Store<T, Store.Result> wrapped;
    private final int maxPageSize;
    private final Function<T, Hasher> hasherFunc;
    private BloomFilter gatekeeper;
    private int delCount;

    public static Shape calculateShape(int maxPageSize) {
        int k = (int) Math.floor(10 + Math.log10(maxPageSize));
        int m = (int) Math.floor(k * maxPageSize / Math.log(2));
        return Shape.fromKM(k, m);
    }

    public StringPage(Store<T, Store.Result> wrapped, int maxPageSize, Function<T, Hasher> hasherFunc) {
        this.wrapped = wrapped;
        this.maxPageSize = maxPageSize;
        this.hasherFunc = hasherFunc;
        gatekeeper = new SimpleBloomFilter(calculateShape(maxPageSize));
        delCount = 0;
    }

    @Override
    public Result delete(T item, BloomFilter filter) {
        if (gatekeeper.contains(filter)) {
            Result result = wrapped.delete(item);
            if (result.existed) {
                delCount++;
                if (wrapped.size() + delCount > maxPageSize * 1.1) {
                    rebuildFilter();
                }
            }
            return result;
        }
        return Store.NO_RESULT;
    }

    @Override
    public Result register(T item, BloomFilter filter) {
        if (!hasSpace()) {
            if (contains(item, filter)) {
                return wrapped.register(item);
            }
            return Store.NO_RESULT;
        }
        Result result = wrapped.register(item);
        if (result.wasAdded()) {
            gatekeeper.merge(filter);
        }
        return result;
    }

    @Override
    public boolean contains(T item, BloomFilter filter) {
        if (gatekeeper.contains(filter)) {
            return wrapped.contains(item);
        }
        return false;
    }

    @Override
    public long size() {
        return wrapped.size();
    }

    @Override
    public boolean mightContain(BloomFilter filter) {
        return gatekeeper.contains(filter);
    }

    @Override
    public boolean hasSpace() {
        return size() < maxPageSize;
    }

    @Override
    public T get(long idx) {
        return wrapped.get(idx);
    }

    private void rebuildFilter() {
        BloomFilter newKeeper = new SimpleBloomFilter(gatekeeper.getShape());
        Iterator<IdxData<T>> iter = wrapped.iterator();
        while (iter.hasNext()) {
            newKeeper.merge(hasherFunc.apply(iter.next().data));
        }
        gatekeeper = newKeeper;
        delCount = 0;
    }

    @Override
    public Iterator<IdxData<T>> iterator() {
        return wrapped.iterator();
    }
}
