package org.xenei.rdfstore.idx;

import java.util.HashMap;
import java.util.Map;

import org.xenei.rdfstore.IdxData;

/**
 * An index of numbers to uri indices.
 */
public class NumberIdx implements Index<Number> {

    Map<Number, IdxData<Bitmap>> map = new HashMap<>();

    
    @Override
    public Bitmap register(Number item, long id) {
        checkIndex( id, Integer.MAX_VALUE );
        IdxData<Bitmap> idx = map.get(item);
        if (idx == null) {
            idx = new IdxData<Bitmap>(id, new Bitmap());
        }
        idx.data.set(id);
        return idx.data;
    }

    @Override
    public void delete(Number item, long id) {
        checkIndex( id, Integer.MAX_VALUE );
        IdxData<Bitmap> idx = map.get(item);
        if (idx != null) {
            idx.data.clear(id);
            if (idx.data.isEmpty()) {
                map.remove(item, idx);
            }
        }
    }

    @Override
    public Bitmap get(Number item) {
        IdxData<Bitmap> idx = map.get(item);
        return idx == null ? new Bitmap() : idx.data;
    }

    @Override
    public long size() {
        return map.size();
    }
}
