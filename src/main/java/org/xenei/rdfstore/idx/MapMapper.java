package org.xenei.rdfstore.idx;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.IdxData;

public class MapMapper<T> implements Mapper<T> {
    Map<T, IdxData<Bitmap>> wrapped;

    public MapMapper(Map<T, IdxData<Bitmap>> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public long size() {
        return wrapped.size();
    }

    @Override
    public void put(T item, IdxData<Bitmap> idx) {
        wrapped.put(item, idx);
    }

    @Override
    public IdxData<Bitmap> get(T item) {
        return wrapped.get(item);
    }

    @Override
    public void putAll(Mapper<T> other) {
        if (other instanceof MapMapper) {
            wrapped.putAll(((MapMapper<T>) other).wrapped);
        } else {
            AbstractIndex.addAll(other, this);
        }
    }

    @Override
    public void remove(T thing) {
        wrapped.remove(thing);
    }

    @Override
    public Iterator<Entry<T, IdxData<Bitmap>>> iterator() {
        return wrapped.entrySet().iterator();
    }
}