package org.xenei.rdfstore.idx;

import java.util.Iterator;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.TreeMap;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.IndexProducer;

public class Bitmap {

    private TreeMap<Integer, Entry> entries = new TreeMap<Integer, Entry>();

    public static Bitmap union(Bitmap... maps) {
        Bitmap result = new Bitmap();
        for (Bitmap map : maps) {
            Integer key = map.entries.firstKey();
            while (key != null) {
                result.entries.get(key).union(map.entries.get(key));
                key = map.entries.higherKey(key);
            }
        }
        return result;
    }

    public static Bitmap intersection(Bitmap... maps) {
        Bitmap result = new Bitmap();
        // check for any null entries
        for (Bitmap m : maps) {
            if (m == null) {
                return result;
            }
        }

        Integer key = maps[0].entries.firstKey();
        for (Bitmap map : maps) {
            Integer testKey = map.entries.firstKey();
            if (key > testKey) {
                key = testKey;
            }
        }
        Integer nextKey = null;
        while (key != null) {
            Entry entry = result.entries.get(key);
            for (Bitmap map : maps) {
                Integer testKey = map.entries.higherKey(key);
                nextKey = nextKey == null ? testKey : (nextKey > testKey ? testKey : nextKey);
                entry.intersection(map.entries.get(key));
            }
            key = nextKey;
            nextKey = null;
        }
        return result;
    }

    /** A bit shift to apply to an integer to divided by 64 (2^6). */
    private static final int DIVIDE_BY_64 = 6;

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
    public boolean contains(final int bitIndex) {
        Entry entry = entries.get(Integer.valueOf(getLongIndex(bitIndex)));
        return entry == null ? false : entry.contains(bitIndex);
    }

    /**
     * Returns the index of the lowest enabled bit.
     * @return the index of the lowest enabled bit.
     */
    public int lowest() {
        if (entries.isEmpty()) {
            return -1;
        }
        Map.Entry<Integer, Bitmap.Entry> entry = entries.firstEntry();
        long idx = entry.getValue().bitMap;
        int result = 0;
        while ((idx & 0x01L) == 0) {
            idx = idx >> 1;
            result++;
            if (result > 64) {
                throw new IllegalStateException("Bit count too large");
            }
        }
        return entry.getKey().intValue() * 64 + result;
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
    public void set(final int bitIndex) {
        Integer entryIndex = Integer.valueOf(getLongIndex(bitIndex));
        Entry entry = entries.get(entryIndex);
        if (entry == null) {
            entry = new Entry(entryIndex);
            entries.put(entryIndex, entry);
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
    public void clear(final int bitIndex) {

        Integer entryIdx = Integer.valueOf(getLongIndex(bitIndex));
        Entry entry = entries.get(entryIdx);
        if (entry != null) {
            entry.clear(bitIndex);
            if (entry.isEmpty()) {
                entries.remove(entryIdx);
            }
        }
    }

    public PrimitiveIterator.OfInt iterator() {
        return new Iter();
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
    public static int getLongIndex(final int bitIndex) {
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
    public static long getLongBit(final int bitIndex) {
        // Bit shifts only use the first 6 bits. Thus it is not necessary to mask this
        // using 0x3f (63) or compute bitIndex % 64.
        // Note: If the index is negative the shift will be (64 - (bitIndex & 0x3f)) and
        // this will identify an incorrect bit.
        return 1L << bitIndex;
    }

    public static class Entry implements Comparable<Entry> {
        // integer as an unsigned integer
        Integer index;
        long bitMap;

        Entry(Integer index) {
            this(index, 0L);
        }

        Entry(Integer index, long bitMap) {
            this.index = index;
            this.bitMap = bitMap;
        }

        @Override
        public int compareTo(Entry arg0) {
            return Integer.compareUnsigned(index, arg0.index);
        }

        public boolean contains(int bitIndex) {
            return (this.bitMap & getLongBit(bitIndex)) != 0;
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
        public void set(final int bitIndex) {
            this.bitMap |= getLongBit(bitIndex);
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
        public void clear(final int bitIndex) {
            this.bitMap &= ~getLongBit(bitIndex);
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
                this.bitMap |= entry.bitMap;
            }
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
                this.bitMap &= entry.bitMap;
            }
        }
    }

    private class Iter implements PrimitiveIterator.OfInt {
        Iterator<Entry> iterE = entries.values().iterator();
        Entry entry = null;
        PrimitiveIterator.OfInt idxIter = null;

        @Override
        public boolean hasNext() {
            if (idxIter == null || !idxIter.hasNext()) {
                idxIter = nextIdxIter();
            }
            return idxIter.hasNext();
        }

        private PrimitiveIterator.OfInt nextIdxIter() {
            entry = nextEntry();
            return new IdxIter();
        }

        private class IdxIter implements PrimitiveIterator.OfInt {
            final int[] values;
            int pos;
            final int offset;

            IdxIter() {
                if (entry == null) {
                    values = new int[0];
                } else {
                    values = IndexProducer.fromBitMapProducer(BitMapProducer.fromBitMapArray(entry.bitMap))
                            .asIndexArray();
                }
                pos = 0;
                offset = entry.index.intValue();
            }

            @Override
            public boolean hasNext() {
                return pos < values.length;
            }

            @Override
            public int nextInt() {
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
        public int nextInt() {
            return idxIter.nextInt();
        }
    }
}
