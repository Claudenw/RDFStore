package org.xenei.rdfstore.disk;

import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.rdfstore.store.IdxData;
import org.xenei.rdfstore.store.LongList;
import org.xenei.rdfstore.txn.TxnHandler;
import org.xenei.rdfstore.txn.TxnId;

public class DiskLongList<T> implements LongList<T> {
    private final Serde<T> serde;
    private final TxnHandler txnHandler;

    private final RandomAccessFile data;
    private final RandomAccessFile idx;
    private final NavigableSet<DelRecord> del;
    private final Executor executor;

    private File tmpFile;
    private RandomAccessFile txnData;
    private List<TxnHeader> txnHeaders;
    private List<DelRecord> txnUsedDel;
    private List<DelRecord> txnDel;
    private long txnDataPos;
    private long txnIdx;

    DiskLongList(String fileName, Serde<T> serde) throws IOException {
        this.serde = serde;
        data = new RandomAccessFile(fileName + ".dat", "rw");
        idx = new RandomAccessFile(fileName + ".idx", "rw");
        del = new TreeSet<>();
        executor = Executors.newSingleThreadExecutor();
        txnHandler = new TxnHandler(() -> "DiskLongList", this::prepareBegin, this::execCommit, this::execAbort,
                this::execEnd);
        executor.execute(new DelTreeBuilder());
    }

    private <X> X exH(IOSupplier<X> supplier) {
        try {
            return supplier.run();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void exH(IOExec exec) {
        try {
            exec.run();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void prepareBegin(ReadWrite readWrite) {
        if (readWrite == WRITE) {
            exH(() -> {
                tmpFile = File.createTempFile("dll", ".dat");
                txnData = new RandomAccessFile(tmpFile.getAbsolutePath(), "rw");
            });
            txnHeaders = new ArrayList<>();
            txnUsedDel = new ArrayList<>();
            txnDel = new ArrayList<>();
        }
        exH(() -> {
            txnDataPos = data.length();
            txnIdx = idx.length() / Long.BYTES;
        });
    }

    private void execCommit() {
        for (TxnHeader txnHeader : txnHeaders) {
            if (txnHeader.isDeleted()) {
                // deleted data entry
                del.add(txnHeader.asDelRecord());
                txnHeader.write(data, txnHeader.pos);
            } else if (txnHeader.isUpdate()) {
                // updated data entry
                exH(() -> {
                    ByteBuffer bb = txnData.getChannel().map(FileChannel.MapMode.READ_WRITE, txnHeader.txnDataPos,
                            txnHeader.len);
                    data.getChannel()
                            .map(FileChannel.MapMode.READ_WRITE, txnHeader.pos + DataHeader.SIZE, txnHeader.len)
                            .put(bb);
                });
            } else if (txnHeader.isNew()) {
                exH(() -> {
                    txnHeader.pos = data.length();
                    ByteBuffer bb = txnData.getChannel().map(FileChannel.MapMode.READ_WRITE, txnHeader.txnDataPos,
                            txnHeader.len);
                    idx.seek(txnHeader.idx * Long.BYTES);
                    idx.writeLong(txnHeader.pos);
                    data.seek(txnHeader.pos);
                    txnHeader.write(data, txnHeader.pos);
                    data.getChannel()
                            .map(FileChannel.MapMode.READ_WRITE, txnHeader.pos + DataHeader.SIZE, txnHeader.len)
                            .put(bb);
                });
            }
        }
        del.addAll(txnDel);
        execAbort();
    }

    private void execAbort() {
        exH(() -> {

            try {
                txnData.close();
            } finally {
                tmpFile.delete();
                txnHeaders = null;
                txnUsedDel = null;
                txnDel = null;
                execEnd();
            }
        });
    }

    private void execEnd() {
        txnDataPos = -1;
        txnIdx = -1;
    }

    /**
     * The memory list of deleted records
     *
     */
    private static class DelRecord implements Comparable<DelRecord> {
        long pos;
        int len;

        DelRecord(long pos, int len) {
            this.pos = pos;
            this.len = len;
        }

        @Override
        public int compareTo(DelRecord other) {
            int result = Integer.compare(len, other.len);
            return result == 0 ? Long.compare(pos, other.pos) : result;
        }
    }

    /**
     * The data record
     */
    private static class DataHeader {
        final int len;
        boolean deleted;
        final static int SIZE = Integer.BYTES + 1;

        DataHeader(int len, boolean deleted) {
            this.len = len;
            this.deleted = deleted;
        }

        DataHeader(RandomAccessFile file, long pos) {
            try {
                file.seek(pos);
                len = file.readInt();
                deleted = file.readBoolean();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void write(RandomAccessFile file, long pos) {
            try {
                file.seek(pos);
                file.writeInt(len);
                file.writeBoolean(deleted);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class TxnHeader extends DataHeader {
        /**
         * The position of the data in the Data file
         */
        long pos;
        /**
         * The index for the data.
         */
        long idx;

        long txnDataPos;

        TxnHeader(int len, boolean deleted) {
            this(len, deleted, -1);
        }

        /**
         * 
         * @param len
         * @param deleted
         * @param pos
         */
        TxnHeader(int len, boolean deleted, long pos) {
            super(len, deleted);
            this.pos = pos;
            txnDataPos = -1;
        }

        TxnHeader(DataHeader dataRecordHeader, long pos) {
            this(dataRecordHeader.len, false, pos);
        }

        boolean isNew() {
            return !deleted && pos < 0 && txnDataPos > -1;
        }

        boolean isDeleted() {
            return deleted && pos > -1;
        }

        boolean isUpdate() {
            return !deleted && pos > -1 && txnDataPos > -1;
        }

        DelRecord asDelRecord() {
            return pos > -1 ? new DelRecord(pos, len) : null;
        }
    }

    private TxnHeader search(int len) {
        DelRecord searcher = new DelRecord(len, 0);
        DelRecord searchResult = del.ceiling(searcher);
        if (searchResult != null) {
            del.remove(searchResult);
            txnUsedDel.add(searchResult);
            /*
            if (searchResult.len > 2*len) {
                // remove the excess data
                int blockLen = DataRecordHeader.SIZE + len;
                DataRecordHeader newHeader = new DataRecordHeader(searchResult.len - blockLen - DataRecordHeader.SIZE, true);
                long newPos = searchResult.pos + newHeader.SIZE;
                newHeader.write( data, newPos);
                DataRecordHeader header = new DataRecordHeader( len, false );
                header.write( data, searchResult.pos);
            }
            */
            TxnHeader header = new TxnHeader(searchResult.len, false, searchResult.pos);
            txnHeaders.add(header);
            return header;
        }
        return null;
    }

    /**
     * find a header with space may or may not have buffer space written.
     * 
     * @param len the lenght of the space to find
     * @return DataRecordHeader for the space.
     */
    private TxnHeader findSpace(int len) {
        TxnHeader header = search(len);
        if (header == null) {
            header = new TxnHeader(len, false);
            header.txnDataPos = txnDataPos;
            txnHeaders.add(header);

            txnDataPos += len + DataHeader.SIZE;
        }
        return header;
    }

    private void writeTxn(TxnHeader txnHeader, ByteBuffer buffer) {
        exH(() -> {
            txnHeader.txnDataPos = txnData.length();
            txnData.getChannel().map(FileChannel.MapMode.READ_WRITE, txnHeader.txnDataPos, buffer.capacity())
                    .put(buffer);
        });
    }

    private T readTxn(TxnHeader txnHeader) {
        exH(() -> {
            txnData.seek(txnHeader.txnDataPos);
        });
        return serde.deserialize(txnData);

    }

    @Override
    public IdxData<T> add(T item) {
        ByteBuffer buff = serde.serialize(item);
        return txnHandler.doInTxn(WRITE, () -> {
            TxnHeader txnHeader = findSpace(buff.capacity());
            /*            data.seek(txnHeader.pos+DataRecordHeader.SIZE);
            data.getChannel().map(FileChannel.MapMode.READ_WRITE, pos+DataRecordHeader.SIZE, buff.capacity()).put(buff);
            long id = idx.length()/Long.BYTES;
            idx.seek( idx.length() );
            idx.writeLong(pos);
            */
            txnHeader.idx = txnIdx++;
            writeTxn(txnHeader, buff);
            return new IdxData<T>(txnHeader.idx, item);
        });
    }

    @Override
    public long size() {
        return txnHandler.doInTxn(READ, () -> {
            return txnIdx;
        });
    }

    @Override
    public void set(IdxData<T> item) {
        ByteBuffer buff = serde.serialize(item.data);
        txnHandler.doInTxn(WRITE, () -> {
            exH(() -> {
                idx.seek(item.idx * Long.BYTES);
                long pos = idx.readLong();
                DataHeader header = new DataHeader(data, pos);
                TxnHeader txnHeader = null;
                if (header.len >= buff.capacity()) {
                    txnHeader = new TxnHeader(header, pos);
                    txnHeader.idx = item.idx;
                    txnHeaders.add(txnHeader);
                } else {
                    txnHeader = findSpace(buff.capacity());
                }
                writeTxn(txnHeader, buff);
            });
        });
    }

    private T readData(long pos) {
        DataHeader header = new DataHeader(data, pos);
        return header.deleted ? null : serde.deserialize(data);
    }

    @Override
    public T get(long idx) {
        return txnHandler.doInTxn(READ, () -> {
            if (idx > txnIdx) {
                return null;
            }
            for (TxnHeader txnHeader : txnHeaders) {
                if (txnHeader.idx == idx) {
                    return readTxn(txnHeader);
                }
            }
            return exH(() -> {
                this.idx.seek(idx * Long.BYTES);
                return readData(this.idx.readLong());
            });
        });
    }

    @Override
    public void remove(long idx) {
        txnHandler.doInTxn(READ, () -> {
            if (idx > txnIdx) {
                return;
            }

            for (TxnHeader txnHeader : txnHeaders) {
                if (txnHeader.idx == idx) {
                    txnHeader.deleted = true;
                    return;
                }
            }
            exH(() -> {
                if (idx * Long.BYTES >= this.idx.length()) {
                    return;
                }

                this.idx.seek(idx * Long.BYTES);
                long pos = this.idx.readLong();
                DataHeader header = new DataHeader(data, pos);
                TxnHeader txnHeader = new TxnHeader(header, pos);
                txnHeader.deleted = true;
                txnHeaders.add(txnHeader);
            });
        });
    }

    class IterRec {
        int idx;
        long pos;

        IterRec(int idx, long pos) {
            this.idx = idx;
            this.pos = pos;
        }
    }

    @Override
    public ExtendedIterator<IdxData<T>> iterator() {

        return WrappedIterator.create(new Iterator<IdxData<T>>() {

            private Iterator<IterRec> innerIter = new Iterator<IterRec>() {
                int pos = 0;

                @Override
                public boolean hasNext() {
                    return exH(() -> pos < idx.length());
                }

                @Override
                public IterRec next() {

                    return exH(() -> {
                        if (pos < idx.length()) {
                            idx.seek(pos);
                            return new IterRec(pos++, idx.readLong());
                        }
                        throw new NoSuchElementException();
                    });
                }
            };

            IterRec nextRec = null;

            private boolean findNext() {
                while (nextRec != null) {
                    if (innerIter.hasNext()) {
                        IterRec rec = innerIter.next();
                        DataHeader header = new DataHeader(data, rec.pos);
                        if (!header.deleted) {
                            nextRec = rec;
                        }
                    } else {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                return nextRec != null || findNext();
            }

            @Override
            public IdxData<T> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                exH(() -> data.seek(nextRec.pos + DataHeader.SIZE));
                return new IdxData<T>(nextRec.idx, serde.deserialize(data));
            }
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

    @Override
    public void setTxnId(TxnId prefix) {
        txnHandler.setTxnId(prefix);
    }

    private class DelTreeBuilder implements Runnable {

        @Override
        public void run() {
            long limit = exH(() -> data.length());
            long[] pos = { 0 };
            while (pos[0] < limit) {
                txnHandler.doInTxn(READ, () -> {
                    exH(() -> {
                        data.seek(pos[0]);
                        int len = data.readInt();
                        boolean deleted = data.readBoolean();
                        if (deleted) {
                            del.add(new DelRecord(pos[0], len));
                        }
                        pos[0] = data.getFilePointer() + len;
                    });
                });
            }
        }

    }

    @FunctionalInterface
    public interface IOExec {
        void run() throws IOException;
    }

    @FunctionalInterface
    public interface IOSupplier<T> {
        T run() throws IOException;
    }
}
