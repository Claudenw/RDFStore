package org.xenei.rdfstore.store;

import static java.lang.ThreadLocal.withInitial;
import static org.apache.jena.query.ReadWrite.WRITE;
import static org.apache.jena.query.ReadWrite.READ;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.shared.Lock;
import org.apache.jena.shared.LockMRPlusSW;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.Transactional.Promote;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.rdfstore.IdxData;
import org.xenei.rdfstore.LongList;
import org.xenei.rdfstore.Store;
import org.xenei.rdfstore.TrieStore;
import org.xenei.rdfstore.idx.Bitmap;
import org.xenei.rdfstore.txn.TxnExec;

public class Quads implements Transactional, AutoCloseable {
    private final URIs uris;
    private final TrieStore<ByteBuffer> store;
    // map of uriIdx to triples.
    private final LongList<Bitmap>[] maps;

    @SuppressWarnings("unchecked")
    public Quads() {
        uris = new URIs();
        store = new TrieStore<ByteBuffer>(ByteBuffer::toString);
        maps = new LongList[Idx.values().length];

        for (Idx idx : Idx.values()) {
            maps[idx.ordinal()] = new LongList<Bitmap>();
        }

    }

    /**
     * This lock imposes the multiple-reader and single-writer policy of
     * transactions
     */
    private final Lock transactionLock = new LockMRPlusSW();

    /**
     * Transaction lifecycle operations must be atomic, especially
     * {@link Transactional#begin} and {@link Transactional#commit}.
     * <p>
     * There are changes to be made to several datastructures and this insures that
     * they are made consistently.
     * <p>
     * Lock order must be writer lock, system lock. If a transaction takes the
     * writerLock, it is a writer, and there is only one writer.
     */
    private final ReentrantLock systemLock = new ReentrantLock(true);

    /**
     * Dataset version. A write transaction increments this in commit.
     */
    private final AtomicLong generation = new AtomicLong(0);
    private final ThreadLocal<Long> version = withInitial(() -> 0L);

    private final ThreadLocal<Boolean> isInTransaction = withInitial(() -> false);

    private final ThreadLocal<TxnType> transactionType = withInitial(() -> null);
    // Current state.
    private final ThreadLocal<ReadWrite> transactionMode = withInitial(() -> null);

    // ** TRANSACTION CODE

    @Override
    public boolean isInTransaction() {
        return isInTransaction.get();
    }

    protected void isInTransaction(final boolean b) {
        isInTransaction.set(b);
    }

    /**
     * @return the current mode of the transaction in progress
     */
    @Override
    public ReadWrite transactionMode() {
        return transactionMode.get();
    }

    @Override
    public TxnType transactionType() {
        return transactionType.get();
    }

    private void transactionMode(final ReadWrite readWrite) {
        transactionMode.set(readWrite);
    }

    private static void withLock(java.util.concurrent.locks.Lock lock, Runnable action) {
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    /** Called transaction start code at most once per transaction. */
    private void startTransaction(TxnType txnType, ReadWrite mode) {
        transactionLock.enterCriticalSection(mode.equals(ReadWrite.READ)); // get the dataset write lock, if needed.
        transactionType.set(txnType);
        transactionMode(mode);
        isInTransaction(true);
    }

    /** Called transaction ending code at most once per transaction. */
    private void finishTransaction() {
        isInTransaction.remove();
        transactionType.remove();
        transactionMode.remove();
        version.remove();
        transactionLock.leaveCriticalSection();
    }

    private void _promote(boolean readCommited) {
        // Outside lock.
        if (!readCommited && version.get() != generation.get()) {
            // This tests for any committed writers since this transaction started.
            // This does not catch the case of a currently active writer
            // that has not gone to commit or abort yet.
            // The final test is after we obtain the transactionLock.
            throw new JenaTransactionException("Dataset changed - can't promote");
        }

        // Blocking on other writers.
        transactionLock.enterCriticalSection(Lock.WRITE);
        // Check again now we are inside the lock.
        if (!readCommited && version.get() != generation.get()) {
            // Can't promote - release the lock.
            transactionLock.leaveCriticalSection();
            throw new JenaTransactionException("Concurrent writer changed the dataset : can't promote");
        }
        // We have the writer lock and we have promoted!
        withLock(systemLock, () -> {
            Arrays.stream(maps).forEach(t -> t.begin(WRITE));
            store.begin(WRITE);
            uris.begin(WRITE);
            transactionMode(WRITE);
            if (readCommited)
                version.set(generation.get());
        });
    }

    @Override
    public void begin(TxnType txnType) {
        if (isInTransaction())
            throw new JenaTransactionException("Transactions cannot be nested!");
        _begin(txnType, TxnType.initial(txnType));
    }

    private void _begin(TxnType txnType, ReadWrite readWrite) {
        // Takes Writer lock first, then system lock.
        startTransaction(txnType, readWrite);
        withLock(systemLock, () -> {
            Arrays.stream(maps).forEach(t -> t.begin(readWrite));
            store.begin(WRITE);
            uris.begin(WRITE);
            version.set(generation.get());
        });
    }

    @Override
    public boolean promote(Promote promoteMode) {
        if (!isInTransaction())
            throw new JenaTransactionException("Tried to promote outside a transaction!");
        if (transactionMode().equals(ReadWrite.WRITE))
            return true;

        if (transactionType() == TxnType.READ)
            return false;

        boolean readCommitted = (promoteMode == Promote.READ_COMMITTED);

        try {
            _promote(readCommitted);
            return true;
        } catch (JenaTransactionException ex) {
            return false;
        }
    }

    @Override
    public void commit() {
        if (!isInTransaction())
            throw new JenaTransactionException("Tried to commit outside a transaction!");
        if (transactionMode().equals(WRITE))
            _commit();
        finishTransaction();
    }

    private void _commit() {
        withLock(systemLock, () -> {
            Arrays.stream(maps).forEach(t -> t.commit());
            store.commit();
            uris.commit();
            if (transactionMode().equals(WRITE)) {
                if (version.get() != generation.get())
                    throw new InternalErrorException(
                            String.format("Version=%d, Generation=%d", version.get(), generation.get()));
                generation.incrementAndGet();
            }
        });
    }

    @Override
    public void abort() {
        if (!isInTransaction())
            throw new JenaTransactionException("Tried to abort outside a transaction!");
        if (transactionMode().equals(WRITE))
            _abort();
        finishTransaction();
    }

    private void _abort() {
        withLock(systemLock, () -> {
            Arrays.stream(maps).forEach(t -> t.abort());
            store.abort();
            uris.abort();
        });
    }

    @Override
    public void end() {
        if (isInTransaction()) {
            if (transactionMode().equals(WRITE)) {
                String msg = "end() called for WRITE transaction without commit or abort having been called. This causes a forced abort.";
                // _abort does _end actions inside the lock.
                _abort();
                finishTransaction();
                throw new JenaTransactionException(msg);
            } else {
                _end();
            }
            finishTransaction();
        }
    }

    private void _end() {
        withLock(systemLock, () -> {
            Arrays.stream(maps).forEach(t -> t.end());
            store.end();
            uris.end();
        });
    }

    @Override
    public void close() {
        if (isInTransaction())
            abort();
    }

    private boolean startTxnIfNeeded(ReadWrite readWrite) {
        if (!isInTransaction()) {
            begin(readWrite);
            return true;
        }
        return false;
    }

    public <T> T doInTxn(ReadWrite readWrite, Supplier<T> supplier) {
        boolean started = startTxnIfNeeded(readWrite);
        try {
            T result = supplier.get();
            if (started) {
                commit();
            }
            return result;
        } catch (Exception e) {
            if (started) {
                abort();
            }
            throw e;
        }
    }

    public void doInTxn(ReadWrite readWrite, TxnExec exec) {
        doInTxn(readWrite, () -> {
            exec.exec();
            return null;
        });
    }

    // ** STANDARD CODE

    public long register(Quad quad) {
        return doInTxn(WRITE, () -> {
            if (quad.isTriple()) {
                return register(Quad.create(Quad.defaultGraphNodeGenerated, quad.asTriple()));
            }

            IdxQuad idxQ = new IdxQuad(uris, quad);
            Store.Result result = store.register(idxQ.buffer);
            if (!result.existed) {
                for (Idx idx : Idx.values()) {
                    Bitmap bitmap = new Bitmap();
                    bitmap.set(result.index);
                    maps[idx.ordinal()].set(new IdxData<Bitmap>(idxQ.get(idx), bitmap));
                }
            }

            return result.index;
        });
    }

    public void delete(Quad quad) {
        doInTxn(WRITE, () -> {
            if (quad.isTriple()) {
                delete(Quad.create(Quad.defaultGraphNodeGenerated, quad.asTriple()));
            }
            IdxQuad idxQ = new IdxQuad(uris, quad);
            Store.Result result = store.delete(idxQ.buffer);
            if (result.existed) {
                for (Idx idx : Idx.values()) {
                    maps[idx.ordinal()].remove(idxQ.get(idx));
                }
            }
        });
    }

    public long size() {
        return doInTxn(READ, () -> {
            return store.size();
        });
    }

    private Bitmap merge(Bitmap map, Node n, Idx idx) {
        Bitmap bitmap = null;
        if (n != null) {
            long l = uris.register(n);
            bitmap = maps[idx.ordinal()].get(l);
            if (map == null) {
                return bitmap;
            }
            if (bitmap == null) {
                return map;
            }
        }
        return Bitmap.intersection(map, bitmap);
    }

    public Triple asTriple(IdxQuad idx) {
        return doInTxn(READ, () -> {
            return Triple.create(uris.get(idx.get(Idx.S)), uris.get(idx.get(Idx.P)), uris.get(idx.get(Idx.O)));
        });

    }

    public Quad asQuad(IdxQuad idx) {
        return doInTxn(READ, () -> {
            return Quad.create(uris.get(idx.get(Idx.G)), uris.get(idx.get(Idx.S)), uris.get(idx.get(Idx.P)),
                    uris.get(idx.get(Idx.O)));
        });
    }

    public <T> ExtendedIterator<T> find(Quad quad, Function<IdxQuad, T> mapper) {
        return doInTxn(READ, () -> {
            if (quad.isTriple()) {
                return find(Quad.create(Quad.defaultGraphNodeGenerated, quad.asTriple()), mapper);
            }
            Bitmap bitmap = null;
            for (Idx idx : Idx.values()) {
                Node n = idx.from(quad);
                if (n != null) {
                    bitmap = merge(bitmap, n, idx);
                }
            }
            return WrappedIterator.create(new IdxQuadIterator(bitmap)).mapWith(mapper);
        });
    }

    private IdxQuad getIdxQuad(long quadId) {
        ByteBuffer bb = store.get(quadId);
        if (bb == null) {
            return null;
        }
        return new IdxQuad(bb);
    }

    public Iterator<Node> listNodes(Idx idx) {
        return uris.iterator(IdxData.iterator(maps[idx.ordinal()].iterator()));
    }

    private class IdxQuadIterator implements Iterator<IdxQuad> {

        private PrimitiveIterator.OfLong longIter;
        private IdxQuad next;

        IdxQuadIterator(Bitmap bitmap) {
            longIter = bitmap.iterator();
            next = null;
        }

        @Override
        public boolean hasNext() {
            while (next == null && longIter.hasNext()) {
                next = getIdxQuad(longIter.next());
            }
            return next != null;
        }

        @Override
        public IdxQuad next() {
            IdxQuad result = next;
            next = null;
            return result;
        }
    }

    public static class IdxQuad implements Comparable<IdxQuad> {
        private ByteBuffer buffer;

        IdxQuad(URIs uris, Quad quad) {
            buffer = ByteBuffer.allocate(Long.BYTES * 4);
            buffer.putLong(uris.register(Idx.G.from(quad)));
            buffer.putLong(uris.register(Idx.S.from(quad)));
            buffer.putLong(uris.register(Idx.P.from(quad)));
            buffer.putLong(uris.register(Idx.O.from(quad)));
        }

        IdxQuad(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        long get(Idx idx) {
            return buffer.getLong(idx.bufferPos);
        }

        ByteBuffer getBuffer(Idx idx) {
            ByteBuffer result = buffer.position(idx.bufferPos).slice();
            result.limit(Long.BYTES);
            return result;
        }

        public boolean isBitSet(int bitIndex) {
            return BitMap.contains(buffer.asLongBuffer().array(), bitIndex);
        }

        @Override
        public int compareTo(IdxQuad other) {
            return buffer.compareTo(other.buffer);
        }
    }

}
