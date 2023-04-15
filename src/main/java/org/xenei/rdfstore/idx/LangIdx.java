package org.xenei.rdfstore.idx;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.xenei.rdfstore.GatedList;
import org.xenei.rdfstore.IdxData;
import org.xenei.rdfstore.Store;

public class LangIdx implements Index<String> {
    
    Map<String,IdxData<Bitmap>> map = new HashMap<>();

    @Override
    public Bitmap register(String item, int id) {
        IdxData<Bitmap> idx = map.get(item);
        if (idx == null) {
            idx = new IdxData<Bitmap>( id, new Bitmap() );
        }
        idx.data.set(id);
        return idx.data;
    }

    @Override
    public void delete(String item, int id) {
        IdxData<Bitmap> idx = map.get(item);
        if (idx != null) {
            idx.data.clear(id);
            if (idx.data.isEmpty())
            {
                map.remove(item,idx);
            }
        }
    }

    @Override
    public Bitmap get(String item) {
        IdxData<Bitmap> idx = map.get(item);
        return idx == null?new Bitmap():idx.data;
    }

    @Override
    public int size() {
        return map.size();
    }

    
}
