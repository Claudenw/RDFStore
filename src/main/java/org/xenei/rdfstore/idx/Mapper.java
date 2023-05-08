package org.xenei.rdfstore.idx;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.IdxData;

public interface Mapper<T> {

    long size();

    void put(T item, IdxData<Bitmap> idx);

    IdxData<Bitmap> get(T item);

    default void putAll(Mapper<T> other) {
        AbstractIndex.addAll(other, this);
    }

    void remove(T thing);

    Iterator<Map.Entry<T, IdxData<Bitmap>>> iterator();
}