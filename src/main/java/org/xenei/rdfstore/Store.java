package org.xenei.rdfstore;

import java.util.Iterator;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;

public interface Store<T, R> {

    static final Result NO_RESULT = new Result(false, -1);

    R register(T item);

    R delete(T item);

    T get(long idx);

    boolean contains(T item);

    public long size();

    public Iterator<IdxData<T>> iterator();

    interface Page<T, R> {

        public static Shape calculateShape(int maxPageSize) {
            int k = (int) Math.floor(10 + Math.log10(maxPageSize));
            int m = (int) Math.floor(k * maxPageSize / Math.log(2));
            return Shape.fromKM(k, m);
        }

        public static int calculatePageSize(long totalSize) {
            double d = Math.ceil(Math.sqrt(totalSize));
            long result = (long) d;
            if (result > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Result is too large: " + result + " > " + Integer.MAX_VALUE);
            }
            return (int) result;
        }

        boolean mightContain(BloomFilter filter);

        R delete(T item, BloomFilter filter);

        boolean contains(T item, BloomFilter filter);

        boolean hasSpace();

        R register(T item, BloomFilter filter);

        long size();

        T get(long idx);

        Iterator<IdxData<T>> iterator();
    }

    public static class Result {
        public final boolean existed;
        public final long value;

        public Result(boolean existed, long value) {
            this.existed = existed;
            this.value = value;
        }

        public boolean wasAdded() {
            return (!existed) && value != NO_RESULT.value;
        }
    }
}
