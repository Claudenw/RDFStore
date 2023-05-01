package org.xenei.rdfstore.store;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Supplier;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.IndexProducer;

/**
 * Class to handle a large number of bitmaps.
 */
public interface Bitmap {
    public static final long MAX_UNSIGNED_INT = 0xFFFF_FFFFL;

    public static final long MAX_INDEX = Long.SIZE * MAX_UNSIGNED_INT + 63;

    public static final long NO_INDEX = -1L;

    /** A bit shift to apply to an integer to divided by 64 (2^6). */
    public static final int DIVIDE_BY_64 = 6;

    public static final int PAGE_SIZE = Long.SIZE;

    public static Comparator<Integer> UNSIGNED_COMPARATOR = new Comparator<Integer>() {
        @Override
        public int compare(Integer arg0, Integer arg1) {
            return Integer.compareUnsigned(arg0, arg1);
        }
    };

    /**
     * Calculates the union of the bitmap arguments. Creates a new bitmap instance.
     * 
     * @param maps the bitmaps to calculate the union for.
     * @return a new bitmap.
     */
    public static Bitmap union(Supplier<Bitmap> supplier, Bitmap... maps) {
        Bitmap result = supplier.get();
        for (Bitmap map : maps) {
            Integer key = map.firstKey();
            while (key != null) {
                Entry rEntry = result.get(key);
                if (rEntry == null) {
                    result.put(key, map.get(key));
                } else {
                    rEntry.union(map.get(key));
                }
                key = map.higherKey(key);
            }
        }
        return result;
    }

    public static void copyRemaining(Bitmap dest, Bitmap orig, Integer fromKey) {
        while (fromKey != null) {
            dest.put(fromKey, orig.get(fromKey).clone());
            fromKey = orig.higherKey(fromKey);
        }
    }

    public static Bitmap xor(Supplier<Bitmap> supplier, Bitmap left, Bitmap right) {

        Bitmap result = supplier.get();
        if (left == right) {
            return result;
        }
        if (left == null && right != null) {
            copyRemaining(result, right, right.isEmpty() ? null : right.firstKey());
            return result;
        }
        if (left != null && right == null) {
            copyRemaining(result, left, left.isEmpty() ? null : left.firstKey());
            return result;
        }
        Integer leftPage = left == null || left.isEmpty() ? null : left.firstKey();
        Integer rightPage = right == null || right.isEmpty() ? null : right.firstKey();
        while (leftPage != null && rightPage != null) {
            int i = Integer.compareUnsigned(leftPage, rightPage);
            if (i < 0) {
                result.put(leftPage, left.get(leftPage).clone());
                leftPage = left.higherKey(leftPage);
            } else if (i > 0) {
                result.put(rightPage, right.get(rightPage).clone());
                rightPage = right.higherKey(rightPage);
            } else {
                Entry leftEntry = left.get(leftPage);
                Entry rightEntry = right.get(rightPage);
                Entry newEntry = leftEntry.logical(rightEntry, xor);
                if (!newEntry.isEmpty()) {
                    result.put(leftPage, newEntry);
                }
                leftPage = left.higherKey(leftPage);
                rightPage = right.higherKey(rightPage);
            }
        }
        copyRemaining(result, left, leftPage);
        copyRemaining(result, right, rightPage);

        return result;
    }

    /**
     * Calculates the intersecton between a set of bitmaps.
     * 
     * @param maps the bit maps.
     * @return a bitmap containing the intersection.
     */
    public static Bitmap intersection(Supplier<Bitmap> supplier, Bitmap... maps) {
        Bitmap result = supplier.get();
        if (maps.length == 0) {
            return result;
        }
        // check for any null entries
        for (Bitmap m : maps) {
            if (m == null) {
                return result;
            }
        }

        Integer key = maps[0].firstKey();
        for (Bitmap map : maps) {
            Integer testKey = map.firstKey();
            if (key > testKey) {
                key = testKey;
            }
        }
        Integer nextKey = null;
        while (key != null) {
            Entry entry = maps[0].get(key);
            for (int i = 1; i < maps.length; i++) {
                Integer testKey = maps[i].higherKey(key);
                if (nextKey == null) {
                    nextKey = testKey;
                } else if (testKey != null) {
                    nextKey = nextKey > testKey ? testKey : nextKey;
                }
                Entry other = maps[i].get(key);
                if (other == null) {
                    entry = null;
                    break;
                }
                entry.intersection(other);
            }
            if (entry != null && !entry.isEmpty()) {
                result.put(key, entry);
            }
            key = nextKey;
            nextKey = null;
        }
        return result;
    }

    long pageCount();

    Integer firstKey();

    Integer higherKey(Integer key);

    Entry get(Integer key);

    Entry firstEntry();

    Entry lastEntry();

    Iterator<Entry> entries();

    void put(Integer key, Entry entry);

    void clear();

    void remove(Integer key);

    default void xor(Bitmap other) {
        if (other == null) {
            return;
        }
        if (this == other) {
            this.clear();
        } else {
            Integer thisPage = this.isEmpty() ? null : this.firstKey();
            Integer otherPage = other.isEmpty() ? null : other.firstKey();
            while (thisPage != null && otherPage != null) {
                int i = Integer.compareUnsigned(thisPage, otherPage);
                if (i < 0) {
                    thisPage = this.higherKey(thisPage);
                } else if (i > 0) {
                    this.put(otherPage, other.get(otherPage).clone());
                    otherPage = other.higherKey(otherPage);
                } else {
                    Entry thisEntry = this.get(thisPage);
                    Entry otherEntry = other.get(otherPage);
                    thisEntry.bitMap ^= otherEntry.bitMap;
                    if (thisEntry.isEmpty()) {
                        this.remove(thisPage);
                    }
                    thisPage = this.higherKey(thisPage);
                    otherPage = other.higherKey(otherPage);
                }
            }
            copyRemaining(this, other, otherPage);
        }
    }

    /**
     * Gets the filter index for the specified bit index assuming the filter is
     * using 64-bit longs to store bits starting at index 0.
     *
     * <p>
     * The index is assumed to be positive. For a positive index the result will
     * match {@code bitIndex / 64}.
     * </p>
     *
     * <p>
     * <em>The divide is performed using bit shifts. If the input is negative the
     * behavior is not defined.</em>
     * </p>
     *
     * @param bitIndex the bit index (assumed to be positive)
     * @return the index of the bit map in an array of bit maps.
     */
    public static long getLongIndex(final long bitIndex) {
        // An integer divide by 64 is equivalent to a shift of 6 bits if the integer is
        // positive.
        // We do not explicitly check for a negative here. Instead we use a
        // signed shift. Any negative index will produce a negative value
        // by sign-extension and if used as an index into an array it will throw an
        // exception.
        return bitIndex >> DIVIDE_BY_64;
    }

    /**
     * Gets the filter bit mask for the specified bit index assuming the filter is
     * using 64-bit longs to store bits starting at index 0. The returned value is a
     * {@code long} with only 1 bit set.
     *
     * <p>
     * The index is assumed to be positive. For a positive index the result will
     * match {@code 1L << (bitIndex % 64)}.
     * </p>
     *
     * <p>
     * <em>If the input is negative the behavior is not defined.</em>
     * </p>
     *
     * @param bitIndex the bit index (assumed to be positive)
     * @return the filter bit
     */
    public static long getLongBit(final long bitIndex) {
        // Bit shifts only use the first 6 bits. Thus it is not necessary to mask this
        // using 0x3f (63) or compute bitIndex % 64.
        // Note: If the index is negative the shift will be (64 - (bitIndex & 0x3f)) and
        // this will identify an incorrect bit.
        return 1L << bitIndex;
    }

    default void checkBitIndex(long bitIndex) {
        assert bitIndex <= MAX_INDEX : "Index too large";
    }

    /**
     * Checks if the specified index bit is enabled in the array of bit maps.
     *
     * If the bit specified by bitIndex is not in the bit map false is returned.
     *
     * @param bitMaps The array of bit maps.
     * @param bitIndex the index of the bit to locate.
     * @return {@code true} if the bit is enabled, {@code false} otherwise.
     * @throws IndexOutOfBoundsException if bitIndex specifies a bit not in the
     * range being tracked.
     */
    default boolean contains(final long bitIndex) {
        checkBitIndex(bitIndex);
        Entry entry = get(Integer.valueOf((int) getLongIndex(bitIndex)));
        return entry == null ? false : entry.contains(bitIndex);
    }

    public boolean isEmpty();

    /**
     * Returns the index of the lowest enabled bit.
     * 
     * @return the index of the lowest enabled bit or -1L none are set.
     */
    default long lowest() {
        if (isEmpty()) {
            return NO_INDEX;
        }
        Bitmap.Entry entry = firstEntry();
        long idx = entry.bitMap;
        int result = 0;
        while ((idx & 0x01L) == 0) {
            idx = idx >> 1;
            result++;
            if (result > PAGE_SIZE) {
                throw new IllegalStateException("Bit count too large");
            }
        }
        return Integer.toUnsignedLong(entry.index.intValue()) * PAGE_SIZE + result;
    }

    /**
     * Sets the bit in the bit maps.
     * <p>
     * <em>Does not perform range checking</em>
     * </p>
     *
     * @param bitMaps The array of bit maps.
     * @param bitIndex the index of the bit to set.
     * @throws IndexOutOfBoundsException if bitIndex specifies a bit not in the
     * range being tracked.
     */
    default void set(final long bitIndex) {
        checkBitIndex(bitIndex);
        Integer entryIndex = Integer.valueOf((int) getLongIndex(bitIndex));
        Entry entry = get(entryIndex);
        if (entry == null) {
            entry = new Entry(entryIndex);
            put(entryIndex, entry);
        }
        entry.set(bitIndex);
    }

    /**
     * Sets the bit in the bit maps.
     * <p>
     * <em>Does not perform range checking</em>
     * </p>
     *
     * @param bitMaps The array of bit maps.
     * @param bitIndex the index of the bit to set.
     * @throws IndexOutOfBoundsException if bitIndex specifies a bit not in the
     * range being tracked.
     */
    default void clear(final long bitIndex) {
        checkBitIndex(bitIndex);
        Integer entryIdx = Integer.valueOf((int) getLongIndex(bitIndex));
        Entry entry = get(entryIdx);
        if (entry != null) {
            entry.clear(bitIndex);
            if (entry.isEmpty()) {
                remove(entryIdx);
            }
        }
    }

    default PrimitiveIterator.OfLong iterator() {
        return new Iter(entries());
    }

    @FunctionalInterface
    public interface Logical {
        long apply(long a, long b);
    }

    public static final Logical xor = (a, b) -> a ^ b;
    public static final Logical and = (a, b) -> a & b;
    public static final Logical or = (a, b) -> a | b;

    public static class Entry implements Comparable<Entry> {
        // integer as an unsigned integer
        private final Integer index;
        private long bitMap;

        public Entry(Integer index) {
            this(index, 0L);
        }

        public Entry(Integer index, long bitMap) {
            this.index = index;
            this.bitMap = bitMap;
        }

        public long bitmap() {
            return bitMap;
        }

        public Integer index() {
            return index;
        }

        @Override
        public Entry clone() {
            return new Entry(index, bitMap);
        }

        @Override
        public int compareTo(Entry arg0) {
            return Integer.compareUnsigned(index, arg0.index);
        }

        public boolean contains(long bitIndex) {
            return (this.bitMap & getLongBit(bitIndex)) != 0;
        }

        public void mutate(long other, Logical func) {
            this.bitMap = func.apply(this.bitMap, other);
        }

        /**
         * Sets the bit in the bit maps.
         * <p>
         * <em>Does not perform range checking</em>
         * </p>
         *
         * @param bitMaps The array of bit maps.
         * @param bitIndex the index of the bit to set.
         * @throws IndexOutOfBoundsException if bitIndex specifies a bit not in the
         * range being tracked.
         */
        public void set(final long bitIndex) {
            mutate(getLongBit(bitIndex), or);
        }

        /**
         * Sets the bit in the bit maps.
         * <p>
         * <em>Does not perform range checking</em>
         * </p>
         *
         * @param bitMaps The array of bit maps.
         * @param bitIndex the index of the bit to set.
         * @throws IndexOutOfBoundsException if bitIndex specifies a bit not in the
         * range being tracked.
         */
        public void clear(final long bitIndex) {
            mutate(~getLongBit(bitIndex), and);
        }

        public boolean isEmpty() {
            return this.bitMap == 0;
        }

        /**
         * Updates this entry with the union of the other entry.
         * 
         * @param entry
         */
        public void union(final Entry entry) {
            if (entry != null && this.compareTo(entry) == 0) {
                mutate(entry.bitMap, or);
            }
        }

        public long logical(long bitmap, Logical func) {
            return func.apply(this.bitMap, bitmap);
        }

        public Entry logical(Entry other, Logical func) {
            return new Entry(index, logical(other.bitMap, func));
        }

        /**
         * updates this entry with the intersection with the other entry
         * 
         * @param entry
         */
        public void intersection(final Entry entry) {
            if (entry == null) {
                this.bitMap = 0L;
            } else if (this.compareTo(entry) == 0) {
                mutate(entry.bitMap, and);
            }
        }
    }

    class Iter implements PrimitiveIterator.OfLong {
        private Iterator<Entry> iterE;
        private Entry entry = null;
        private PrimitiveIterator.OfLong idxIter = null;

        Iter(Iterator<Entry> iterE) {
            this.iterE = iterE;
        }

        @Override
        public boolean hasNext() {
            if (idxIter == null || !idxIter.hasNext()) {
                idxIter = nextIdxIter();
            }
            return idxIter.hasNext();
        }

        private PrimitiveIterator.OfLong nextIdxIter() {
            entry = nextEntry();
            return new IdxIter();
        }

        private class IdxIter implements PrimitiveIterator.OfLong {
            final int[] values;
            int pos;
            final long offset; // max offset is MAX_UNSIGNED_INT

            IdxIter() {
                if (entry == null) {
                    values = new int[0];
                    offset = 0;
                } else {
                    values = IndexProducer.fromBitMapProducer(BitMapProducer.fromBitMapArray(entry.bitMap))
                            .asIndexArray();
                    offset = Integer.toUnsignedLong(entry.index) * Long.SIZE;
                }
                pos = 0;
            }

            @Override
            public boolean hasNext() {
                return pos < values.length;
            }

            @Override
            public long nextLong() {
                return values[pos++] + offset;
            }
        }

        private Entry nextEntry() {
            if (iterE.hasNext()) {
                return iterE.next();
            }
            return null;
        }

        @Override
        public long nextLong() {
            return idxIter.nextLong();
        }
    }
}
