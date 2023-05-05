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

//    public static Comparator<Integer> UNSIGNED_COMPARATOR = new Comparator<Integer>() {
//        @Override
//        public int compare(Integer arg0, Integer arg1) {
//            return Integer.compareUnsigned(arg0, arg1);
//        }
//    };

    /**
     * Calculates the union of the bitmap arguments. Creates a new bitmap instance.
     * 
     * @param maps the bitmaps to calculate the union for.
     * @return a new bitmap.
     */
    public static Bitmap union(Supplier<Bitmap> supplier, Bitmap... maps) {
        Bitmap result = supplier.get();
        for (Bitmap map : maps) {
            Key key = map.firstIndex();
            while (key != null) {
                Entry rEntry = result.get(key);
                if (rEntry == null) {
                    result.put(key, map.get(key));
                } else {
                    rEntry.union(map.get(key));
                }
                key = map.higherIndex(key);
            }
        }
        return result;
    }

    public static void copyRemaining(Bitmap dest, Bitmap orig, Key fromKey) {
        while (fromKey != null) {
            dest.put(fromKey, orig.get(fromKey).duplicate());
            fromKey = orig.higherIndex(fromKey);
        }
    }

    public static Bitmap xor(Supplier<Bitmap> supplier, Bitmap left, Bitmap right) {

        Bitmap result = supplier.get();
        if (left == right) {
            return result;
        }
        if (left == null && right != null) {
            copyRemaining(result, right, right.isEmpty() ? null : right.firstIndex());
            return result;
        }
        if (left != null && right == null) {
            copyRemaining(result, left, left.isEmpty() ? null : left.firstIndex());
            return result;
        }
        Key leftKey = left == null || left.isEmpty() ? null : left.firstIndex();
        Key rightkey = right == null || right.isEmpty() ? null : right.firstIndex();
        while (leftKey != null && rightkey != null) {
            int i = leftKey.compareTo(rightkey);
            if (i < 0) {
                result.put(leftKey, left.get(leftKey).duplicate());
                leftKey = left.higherIndex(leftKey);
            } else if (i > 0) {
                result.put(rightkey, right.get(rightkey).duplicate());
                rightkey = right.higherIndex(rightkey);
            } else {
                Entry leftEntry = left.get(leftKey);
                Entry rightEntry = right.get(rightkey);
                long newValue = leftEntry.logical(rightEntry.bitmap(), xor);
                if (newValue != 0) {
                    Entry newEntry = new DefaultEntry( leftKey );
                    newEntry.mutate( newValue, or);
                    result.put(leftKey, newEntry);
                }
                leftKey = left.higherIndex(leftKey);
                rightkey = right.higherIndex(rightkey);
            }
        }
        copyRemaining(result, left, leftKey);
        copyRemaining(result, right, rightkey);

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

        Key key = maps[0].firstIndex();
        for (Bitmap map : maps) {
            Key testKey = map.firstIndex();
            if (key.compareTo(testKey) > 0) {
                key = testKey;
            }
        }
        Key nextKey = null;
        while (key != null) {
            Entry entry = maps[0].get(key);
            for (int i = 1; i < maps.length; i++) {
                Key testKey = maps[i].higherIndex(key);
                if (nextKey == null) {
                    nextKey = testKey;
                } else if (testKey != null) {
                    nextKey = nextKey.compareTo(testKey) > 0 ? testKey : nextKey;
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

    /**
     * gets the index for the first Entry in the collection.
     * @return
     */
    Key firstIndex();

    /**
     * Returns the index for the next higher Entry 
     * @param key
     * @return
     */
    Key higherIndex(Key key);

    Entry get(Key key);

    Entry firstEntry();

    Entry lastEntry();

    Iterator<? extends Entry> entries();

    <T extends Entry> void put(Key key, T entry);

    void clear();

    void remove(Key key);

    default void xor(Bitmap other) {
        if (other == null) {
            return;
        }
        if (this == other) {
            this.clear();
        } else {
            Key thisKey = this.isEmpty() ? null : this.firstIndex();
            Key otherKey = other.isEmpty() ? null : other.firstIndex();
            while (thisKey != null && otherKey != null) {
                
                int i = thisKey.compareTo(otherKey);
                if (i < 0) {
                    thisKey = this.higherIndex(thisKey);
                } else if (i > 0) {
                    this.put(otherKey, other.get(otherKey).duplicate());
                    otherKey = other.higherIndex(otherKey);
                } else {
                    Entry thisEntry = this.get(thisKey);
                    Entry otherEntry = other.get(otherKey);
                    thisEntry.mutate( otherEntry.bitmap(), or );
                    if (thisEntry.isEmpty()) {
                        this.remove(thisKey);
                    }
                    thisKey = this.higherIndex(thisKey);
                    otherKey = other.higherIndex(otherKey);
                }
            }
            copyRemaining(this, other, otherKey);
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
        Entry entry = get( new Key(getLongIndex(bitIndex)));
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
        long idx = entry.bitmap();
        int result = 0;
        while ((idx & 0x01L) == 0) {
            idx = idx >> 1;
            result++;
            if (result > PAGE_SIZE) {
                throw new IllegalStateException("Bit count too large");
            }
        }
        return entry.index().asUnsigned() * PAGE_SIZE + result;
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
        Key entryIndex = new Key( getLongIndex(bitIndex));
        Entry entry = get(entryIndex);
        if (entry == null) {
            entry = new DefaultEntry(entryIndex);
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
        Key entryIdx = new Key( getLongIndex(bitIndex));
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

    public interface Entry extends Comparable<Entry> {
   
        /**
         * Gets the bitmap for this entry.
         * @return
         */
        long bitmap();

        /**
         * Gets the position of this entry in the collection of entries.
         * @return the position of this entry.
         */
        Key index();

        /**
         * Duplicates this entry.  Resulting entry has the same bitmap and index.
         * @return
         */
        Entry duplicate();

        @Override
        default int compareTo(Entry arg0) {
            return index().compareTo(arg0.index());
        }

        /**
         * Returns {@code true} if the bit is set.
         * @param bitIndex the bit to check
         * @return {@code true} if the bit is set.
         */
        default boolean contains(long bitIndex) {
            return (bitmap() & getLongBit(bitIndex)) != 0;
        }

        /**
         * Mutate the bitmap by applying the other and hte logical function to the internal bitmap
         * @param other the other bitmap
         * @param func the function to apply
         */
        public void mutate(long other, Logical func);

        /**
         * Sets the bit in the bit map.
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
        default void clear(final long bitIndex) {
            mutate(~getLongBit(bitIndex), and);
        }

        /**
         * Returns {@code true} if the bitmap is empty.
         * @return
         */
        default boolean isEmpty() {
            return bitmap() == 0;
        }

        /**
         * Updates this entry with the union of the other entry.
         * 
         * @param entry
         */
        default void union(final Entry entry) {
            if (entry != null && compareTo(entry) == 0) {
                mutate(entry.bitmap(), or);
            }
        }

        /**
         * Applies the function to this bitmap and the other bitmap and returnes the resulting bitmap.
         * @param other
         * @param func
         * @return
         */
        default long logical(long other, Logical func) {
            return func.apply(bitmap(), other);
        }

        /**
         * updates this entry with the intersection with the other entry
         * 
         * @param entry
         */
        default void intersection(final Entry entry) {
            if (entry == null) {
                mutate( ~bitmap(), xor);
            } else if (this.compareTo(entry) == 0) {
                mutate(entry.bitmap(), and);
            }
        }
    }

    class Iter implements PrimitiveIterator.OfLong {
        private Iterator<? extends Entry> iterE;
        private Entry entry = null;
        private PrimitiveIterator.OfLong idxIter = null;

        Iter(Iterator<? extends Entry> iterE) {
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
                    values = IndexProducer.fromBitMapProducer(BitMapProducer.fromBitMapArray(entry.bitmap()))
                            .asIndexArray();
                    offset = entry.index().asUnsigned() * Long.SIZE;
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
    
    public class Key implements Comparable<Key> {
        Integer value;
        
        public Key(int value) {
            this.value = value;
        }
        
        public Key(Integer value) {
            this.value = value;
        }

        public Key(long value) {
            this.value = (int) value;
        }
        
        public long asUnsigned() {
            return Integer.toUnsignedLong(value);
        }
        
        public int asSigned() {
            return value.intValue();
        }
        
        @Override
        public int compareTo(Key other) {
            if (value == other.value) {
                return 0;
            }
            if (value == null) {
                return -1;
            }
            if (other.value == null) {
                return 1;
            }
            return Integer.compareUnsigned(this.value, other.value);
        }
    }

    class DefaultEntry implements Entry {
        // integer as an unsigned integer
        private final Key key;
        private long bitMap;
    
        public DefaultEntry(Key key) {
            this(key, 0L);
        }
    
        public DefaultEntry(Key key, long bitMap) {
            this.key = key;
            this.bitMap = bitMap;
        }
    
        @Override
        public long bitmap() {
            return bitMap;
        }
    
        @Override
        public Key index() {
            return key;
        }
    
        @Override
        public DefaultEntry duplicate() {
            return new DefaultEntry(key, bitMap);
        }
    
        @Override
        public void mutate(long other, Logical func) {
            this.bitMap = func.apply(this.bitMap, other);
        }
    }
}
