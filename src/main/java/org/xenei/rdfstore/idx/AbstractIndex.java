package org.xenei.rdfstore.idx;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.function.Consumer;

import org.apache.jena.query.ReadWrite;
import org.xenei.rdfstore.IdxData;
import org.xenei.rdfstore.txn.TxnExec;
import org.xenei.rdfstore.txn.TxnHandler;
import org.xenei.rdfstore.txn.TxnId;

import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;


public class AbstractIndex<T> implements Index<T> {

    private final Map<T, IdxData<Bitmap>> map;
    private final TxnHandler txnHandler;

    public AbstractIndex(TxnId txnId) {
        map = new HashMap<>();
        txnHandler = new TxnHandler(txnId, this::prepareBegin, this::execCommit, this::execAbort, this::execEnd);
    }
    
    public void setTxnId(TxnId prefix) {
        txnHandler.setTxnId(prefix);
    }

    private Map<T,IdxData<Bitmap>> txnAddMap;
    private Set<T> txnDelSet;
    
    void prepareBegin(ReadWrite readWrite) {
        txnAddMap = new HashMap<>();
        txnDelSet = new HashSet<>();
    }
    
    void execCommit() {
        map.putAll( txnAddMap);
        txnDelSet.forEach( key -> {map.remove(key);});
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
        return txnHandler.doInTxn( WRITE, () -> {
            checkIndex(id, Integer.MAX_VALUE);
            IdxData<Bitmap> idx = txnAddMap.get(item);
            if (idx == null && !txnDelSet.contains(item)) {
                idx = map.get(item);
            }
            if (idx == null) {
                txnDelSet.remove(item);
                idx = new IdxData<Bitmap>(id, new Bitmap());
                txnAddMap.put( item, idx );
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
        }});
        
//
//        idx.data.clear(id);
//        if (idx.data.isEmpty()) {
//            map.remove(item, idx);
//        }
//    }
    }

    @Override
    public Bitmap get(T item) {
        return txnHandler.doInTxn(READ, () -> {
        IdxData<Bitmap> idx = map.get(item);
        return idx == null ? new Bitmap() : idx.data;
        });
    }

    @Override
    public long size() {
        return txnHandler.doInTxn(READ, () -> {
            return map.size()+txnAddMap.size()-txnDelSet.size();
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
