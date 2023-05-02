package org.xenei.rdfstore.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.xenei.rdfstore.store.IdxData;
import org.xenei.rdfstore.store.LongList;
import org.xenei.rdfstore.txn.TxnHandler;
import org.xenei.rdfstore.txn.TxnId;

import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;


public class DiskLongList<T> implements LongList<T> {
    private final Serde<T> serde;
    private final TxnHandler txnHandler;
    private final RandomAccessFile data;
    private final RandomAccessFile idx;
    private final RandomAccessFile del;
    private long delCount;
    private long idxCount;
   
   
    DiskLongList(String fileName, Serde<T> serde) throws IOException {
        this.serde = serde;
        data = new RandomAccessFile( fileName+".dat", "rw");
        idx = new RandomAccessFile( fileName+".idx", "rw");
        del = new RandomAccessFile( fileName+".del", "rw");
        delCount = del.length() / Long.BYTES;
        idxCount = idx.length() / Long.BYTES;
        
        txnHandler = new TxnHandler(() -> "DiskLongList", this::prepareBegin, this::execCommit, this::execAbort,
                this::execEnd);
    }

    private void prepareBegin(ReadWrite readWrite) {}
    private void execCommit() {}
    private void execAbort() {}
    private void execEnd() {}
    

    
    @Override
    public IdxData<T> add(T item) {
        txnHandler.doInTxn(WRITE, () -> {
            long index;
            if (delCount > 0) {
                long pos = (delCount-1) * Long.BYTES; 
                del.seek(pos);
                index = del.readLong();
                del.setLength(pos);
                delCount--;
            } else {
                index = idxCount * Long.BYTES;
            }
            long pos = data.length();
            data.seek( pos);
            ByteBuffer buff = serde.serialize(item);
            data.write( buff.limit());
            data.write( buff.array());
            idx.seek(index);
            idx.writeLong(pos);
        });
    }

    @Override
    public long size() {
        return idxCount - delCount;
    }

    @Override
    public void set(IdxData<T> data) {
        // TODO Auto-generated method stub

    }

    @Override
    public T get(long idx) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void remove(long idx) {
        // TODO Auto-generated method stub

    }

    @Override
    public ExtendedIterator<IdxData<T>> iterator() {
        // TODO Auto-generated method stub
        return null;
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

    public void setTxnId(TxnId prefix) {
        txnHandler.setTxnId(prefix);
    }
    
    

}
