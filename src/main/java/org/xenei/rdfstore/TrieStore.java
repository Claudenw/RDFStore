package org.xenei.rdfstore;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.xenei.rdfstore.idx.Bitmap;

import java.util.List;
import java.util.ArrayList;
import org.apache.commons.collections4.Trie;

public class TrieStore<T> implements Store<T,Store.Result> {
   
    private final List<T> lst;
    private final Trie<String,IdxData<T>> trie;
    private final Bitmap deleted;
    
    public TrieStore() {
        lst = new ArrayList<T>();
        trie = new PatriciaTrie<IdxData<T>>();
        deleted = new Bitmap();
    }
    
    @Override
    public Result register(T item) {
        final String key = item.toString();
        IdxData<T> entry = trie.get(key);
        if (entry == null) {
            long deletedIdx = deleted.lowest();
            if (deletedIdx == -1) {
                lst.add(item);
                entry = new IdxData<T>(lst.size(), item);
                trie.put( key, entry);
            } else {
                deleted.clear( deletedIdx );
                entry = new IdxData<T>((int)deletedIdx, item);
                lst.set( entry.idx, item );
                trie.put( key, entry);
            }
            return new Result( false, entry.idx);
        }
        return new Result( true, entry.idx);
    }

    @Override
    public Result delete(T item) {
        final String key = item.toString();
        IdxData<T> idxData = trie.remove(key);
        if (idxData == null) {
            return new Result( false, -1 );
        }
        deleted.set( idxData.idx );
        lst.set(idxData.idx, null);
        return new Result( true, idxData.idx);
    }

    @Override
    public boolean contains(T item) {
        return trie.containsKey( item.toString() );
    }
    
    @Override
    public int size() {
        return trie.size();
    }


}