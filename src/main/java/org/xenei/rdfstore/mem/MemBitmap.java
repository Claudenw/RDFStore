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
    TreeMap<Integer, Entry> entries = new TreeMap<Integer, Entry>(UNSIGNED_COMPARATOR);

    @Override
    public Integer firstKey() {
        return entries.firstKey();
    }

    @Override
    public Integer higherKey(Integer key) {
        return entries.higherKey(key);
    }

    @Override
    public Entry get(Integer key) {
        return entries.get(key);
    }

    @Override
    public Entry firstEntry() {
        return entries.firstEntry().getValue();
    }

    @Override
    public Iterator<Entry> entries() {
        return entries.values().iterator();
    }

    @Override
    public void put(Integer key, Entry entry) {
        entries.put(key, entry);
    }

    @Override
    public void clear() {
        entries.clear();
    }

    @Override
    public void remove(Integer key) {
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
}
