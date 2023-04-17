package org.xenei.rdfstore;

import java.util.Iterator;
import java.util.function.Function;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.xenei.rdfstore.idx.Bitmap;

public class TrieStore<T> implements Store<T> {

    private final LongList<T> lst;
    private final Trie<String, IdxData<T>> trie;
    private final Bitmap deleted;
    private final Function<T, String> keyFunc;

    /**
     * Constructor that uses String::valueOf as the key function.
     */
    public TrieStore() {
        this(String::valueOf);
    }

    /**
     * Constructor that accepts key function.
     * 
     * @param keyFunc the function to convert the item to a string for the key.
     */
    public TrieStore(Function<T, String> keyFunc) {
        lst = new LongList<T>();
        trie = new PatriciaTrie<IdxData<T>>();
        deleted = new Bitmap();
        this.keyFunc = keyFunc;
    }

    @Override
    public Result register(T item) {
        String key = keyFunc.apply(item);
        IdxData<T> entry = trie.get(key);
        if (entry == null) {
            long deletedIdx = deleted.lowest();
            if (deletedIdx == -1) {
                entry = lst.add(item);
                trie.put(key, entry);
            } else {
                deleted.clear(deletedIdx);
                entry = new IdxData<T>((int) deletedIdx, item);
                lst.set(entry);
                trie.put(key, entry);
            }
            return new Result(false, entry.idx);
        }
        return new Result(true, entry.idx);
    }

    @Override
    public Result delete(T item) {
        String key = keyFunc.apply(item);
        IdxData<T> idxData = trie.remove(key);
        if (idxData == null) {
            return new Result(false, -1);
        }
        deleted.set(idxData.idx);
        lst.set(new IdxData<T>( idxData.idx, null));
        return new Result(true, idxData.idx);
    }

    @Override
    public boolean contains(T item) {
        return trie.containsKey(keyFunc.apply(item));
    }

    @Override
    public long size() {
        return trie.size();
    }

    @Override
    public Iterator<IdxData<T>> iterator() {
        return trie.values().iterator();
    }

    @Override
    public T get(long idx) {
        return lst.get(idx);
    }

}
