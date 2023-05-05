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
    TreeMap<Key, Bitmap.DefaultEntry> entries = new TreeMap<Key, Bitmap.DefaultEntry>();

    @Override
    public Key firstIndex() {
        return entries.firstKey();
    }

    @Override
    public Key higherIndex(Key key) {
        return entries.higherKey(key);
    }

    @Override
    public Bitmap.DefaultEntry get(Key key) {
        return entries.get(key);
    }

    @Override
    public Bitmap.DefaultEntry firstEntry() {
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
        Bitmap.DefaultEntry myEntry = (entry instanceof Bitmap.DefaultEntry) ? (Bitmap.DefaultEntry) entry : new Bitmap.DefaultEntry(entry.index(), entry.bitmap());
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
    public Bitmap.DefaultEntry lastEntry() {
        return entries.lastEntry().getValue();
    }
}
