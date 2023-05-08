package org.xenei.rdfstore.idx;

import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.apache.jena.query.ReadWrite;
import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.IdxData;
import org.xenei.rdfstore.store.Index;
import org.xenei.rdfstore.txn.TxnHandler;
import org.xenei.rdfstore.txn.TxnId;

public class AbstractIndex<T> implements Index<T> {

    private final Mapper<T> map;// Map<T, IdxData<Bitmap>> map;
    private final TxnHandler txnHandler;
    private Supplier<Bitmap> bitmapSupplier;
    private Mapper<T> txnAddMap;
    private Set<T> txnDelSet;

    public AbstractIndex(TxnId txnId, Supplier<Bitmap> bitmapSupplier, Mapper<T> map) {
        this.map = map;
        this.bitmapSupplier = bitmapSupplier;
        txnHandler = new TxnHandler(txnId, this::prepareBegin, this::execCommit, this::execAbort, this::execEnd);
    }

    static <T> void addAll(Mapper<T> from, Mapper<T> to) {
        Iterator<Map.Entry<T, IdxData<Bitmap>>> iter = from.iterator();
        while (iter.hasNext()) {
            Map.Entry<T, IdxData<Bitmap>> entry = iter.next();
            to.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void setTxnId(TxnId prefix) {
        txnHandler.setTxnId(prefix);
    }

    void prepareBegin(ReadWrite readWrite) {
        txnAddMap = new MapMapper<T>(new TreeMap<>());
        txnDelSet = new HashSet<>();
    }

    void execCommit() {
        map.putAll(txnAddMap);
        txnDelSet.forEach(key -> {
            map.remove(key);
        });
        txnAddMap = null;
        txnDelSet = null;
    }

    void execAbort() {
        txnAddMap = null;
        txnDelSet = null;
    }

    void execEnd() {
        txnAddMap = null;
        txnDelSet = null;
    }

    @Override
    public Bitmap register(T item, long id) {
        return txnHandler.doInTxn(WRITE, () -> {
            checkIndex(id, Integer.MAX_VALUE);
            IdxData<Bitmap> idx = txnAddMap.get(item);
            if (idx == null && !txnDelSet.contains(item)) {
                idx = map.get(item);
            }
            if (idx == null) {
                txnDelSet.remove(item);
                idx = new IdxData<Bitmap>(id, bitmapSupplier.get());
                txnAddMap.put(item, idx);
            }
            idx.data.set(id);
            return idx.data;
        });
    }

    @Override
    public void delete(T item, long id) {
        txnHandler.doInTxn(WRITE, () -> {
            checkIndex(id, Integer.MAX_VALUE);
            IdxData<Bitmap> idx = map.get(item);
            if (idx == null) {
                idx = txnAddMap.get(item);
            }
            if (idx != null) {
                txnDelSet.add(item);
                txnAddMap.remove(item);
            }
        });
    }

    @Override
    public Bitmap get(T item) {
        return txnHandler.doInTxn(READ, () -> {
            IdxData<Bitmap> idx = map.get(item);
            return idx == null ? bitmapSupplier.get() : idx.data;
        });
    }

    @Override
    public long size() {
        return txnHandler.doInTxn(READ, () -> {
            return map.size() + txnAddMap.size() - txnDelSet.size();
        });
    }

    @Override
    public void begin(ReadWrite readWrite) {
        txnHandler.begin(readWrite);
    }

    @Override
    public void commit() {
        txnHandler.commit();
    }

    @Override
    public void abort() {
        txnHandler.abort();
    }

    @Override
    public void end() {
        txnHandler.end();
    }
}
