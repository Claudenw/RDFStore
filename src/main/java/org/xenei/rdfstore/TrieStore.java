package org.xenei.rdfstore;

import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.jena.query.ReadWrite;
import org.xenei.rdfstore.idx.Bitmap;
import org.xenei.rdfstore.txn.TxnHandler;

public class TrieStore<T> implements Store<T> {

    private final LongList<T> lst;
    private final Trie<String, IdxData<T>> trie;
    private final Bitmap deleted;
    private final Function<T, String> keyFunc;

    private final TxnHandler txnHandler;

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
        txnHandler = new TxnHandler(this::prepareBegin, this::execCommit, this::execAbort, this::execEnd);
    }

    Map<String, IdxData<T>> txnAdd;
    Set<String> txnDel;
    Bitmap txnUsed;

    private void prepareBegin(ReadWrite readWrite) {
        lst.begin(readWrite);
        txnAdd = new HashMap<>();
        txnDel = new HashSet<>();
        txnUsed = new Bitmap();
    }

    private void execCommit() {
        txnAdd.forEach((k, v) -> {
            trie.put(k, v);
            lst.set(v);
        });
        txnDel.forEach((k) -> {
            IdxData<T> removed = trie.remove(k);
            if (removed != null) {
                lst.remove(removed.idx);
            }
        });
        deleted.xor(txnUsed);
        txnAdd = null;
        txnDel = null;
        txnUsed = null;
    }

    private void execAbort() {
        txnDel = null;
        txnAdd = null;
        txnUsed = null;
        lst.abort();
    }

    private void execEnd() {
        txnDel = null;
        txnAdd = null;
        txnUsed = null;
        lst.end();
    }

    @Override
    public Result register(T item) {
        String key = keyFunc.apply(item);
        return txnHandler.doInTxn(READ, () -> {
            IdxData<T> entry = txnAdd.get(key);
            if (entry == null) {
                entry = trie.get(key);
            }
            if (entry == null) {
                Bitmap txnDeleted = Bitmap.xor(txnUsed, deleted);
                long deletedIdx = txnDeleted.lowest();
                if (deletedIdx == -1) {
                    entry = lst.add(item);
                    txnAdd.put(key, entry);
                } else {
                    txnUsed.set(deletedIdx);
                    entry = new IdxData<T>((int) deletedIdx, item);
                    lst.set(entry);
                    txnAdd.put(key, entry);
                }
                txnDel.remove(key);
                return new Result(false, entry.idx);

            }
            return new Result(true, entry.idx);
        });
    }

    @Override
    public Result delete(T item) {
        String key = keyFunc.apply(item);
        return txnHandler.doInTxn(WRITE, () -> {
            if (txnDel.contains(key)) {
                return NO_RESULT;
            }
            IdxData<T> found = txnAdd.remove(key);
            if (found == null) {
                found = trie.get(key);
            }
            if (found != null) {
                txnDel.add(key);
                return new Result(true, found.idx);
            }
            return NO_RESULT;
        });
    }

    @Override
    public boolean contains(T item) {
        return txnHandler.doInTxn(READ, () -> {
            String key = keyFunc.apply(item);
            if (txnDel.contains(key)) {
                return false;
            }
            return (txnAdd.containsKey(key) || trie.containsKey(key));
        });
    }

    @Override
    public long size() {
        return txnHandler.doInTxn(READ, () -> {
            return trie.size() + txnAdd.size() - txnDel.size();
        });
    }

    @Override
    public Iterator<IdxData<T>> iterator() {
        return trie.values().iterator();
    }

    @Override
    public T get(long idx) {
        return txnHandler.doInTxn(READ, () -> {
            return lst.get(idx);
        });
    }

    public void begin(ReadWrite readWrite) {
        txnHandler.begin(readWrite);
    }

    public void commit() {
        txnHandler.commit();
    }

    public void abort() {
        txnHandler.abort();
    }

    public void end() {
        txnHandler.end();
    }

}
