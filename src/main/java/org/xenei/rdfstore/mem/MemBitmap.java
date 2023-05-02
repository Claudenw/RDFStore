package org.xenei.rdfstore.mem;

import java.util.Iterator;
import java.util.TreeMap;

import org.xenei.rdfstore.store.Bitmap;

/**
 * Class to handle a large number of bitmaps.
 */
public class MemBitmap implements Bitmap {

    /**
     * A list of entries
     */
    TreeMap<Key, Entry> entries = new TreeMap<Key, Entry>();

    @Override
    public Key firstIndex() {
        return entries.firstKey();
    }

    @Override
    public Key higherIndex(Key key) {
        return entries.higherKey(key);
    }

    @Override
    public Entry get(Key key) {
        return entries.get(key);
    }

    @Override
    public Entry firstEntry() {
        return entries.firstEntry().getValue();
    }

    @Override
    public Iterator<? extends Bitmap.Entry> entries() {
        return entries.values().iterator();
        // return WrappedIterator.create(entries.values().iterator()).mapWith( e ->
        // (Bitmap.Entry)e);
    }

    @Override
    public <T extends Bitmap.Entry> void put(Key key, T entry) {
        Entry myEntry = (entry instanceof Entry) ? (Entry) entry : new Entry(entry.index(), entry.bitmap());
        entries.put(key, myEntry);
    }

    @Override
    public void clear() {
        entries.clear();
    }

    @Override
    public void remove(Key key) {
        entries.remove(key);
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public long pageCount() {
        return entries.size();
    }

    @Override
    public Entry lastEntry() {
        return entries.lastEntry().getValue();
    }

    @Override
    public Entry createEntry(Key key) {
        return new Entry(key);
    }

    public static class Entry implements Bitmap.Entry {
        // integer as an unsigned integer
        private final Key key;
        private long bitMap;

        public Entry(Key key) {
            this(key, 0L);
        }

        public Entry(Key key, long bitMap) {
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
        public Entry duplicate() {
            return new Entry(key, bitMap);
        }

        @Override
        public void mutate(long other, Logical func) {
            this.bitMap = func.apply(this.bitMap, other);
        }
    }

}
