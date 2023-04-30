package org.xenei.rdfstore.store;

import static org.apache.jena.query.TxnType.READ;
import static org.apache.jena.query.TxnType.WRITE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.rdfstore.IdxData;
import org.xenei.rdfstore.LongList;
import org.xenei.rdfstore.Store;
import org.xenei.rdfstore.TrieStore;
import org.xenei.rdfstore.idx.Bitmap;
import org.xenei.rdfstore.txn.TxnController;
import org.xenei.rdfstore.txn.TxnId;

public class Quads implements Transactional, AutoCloseable {
    private final URIs uris;
    private final TrieStore<ByteBuffer> store;
    // map of uriIdx to triples.
    private final LongList<Bitmap>[] maps;

    private final static Logger LOG = LoggerFactory.getLogger(Quads.class);

    private final TxnController txnController;

    @SuppressWarnings("unchecked")
    public Quads() {
        TxnId txnId = () -> "Quads";
        uris = new URIs();
        uris.setTxnId(txnId);
        store = new TrieStore<ByteBuffer>(ByteBuffer::toString);
        store.setTxnId(txnId);
        maps = new LongList[Idx.values().length];

        for (Idx idx : Idx.values()) {
            maps[idx.ordinal()] = new LongList<Bitmap>();
            maps[idx.ordinal()].setTxnId(TxnId.setParent(txnId, () -> "map" + idx.ordinal()));
        }

        txnController = new TxnController(txnId, this::prepareBegin, this::commitF, this::abortF, this::endF);
    }

    private void prepareBegin(ReadWrite readWrite) {
        Arrays.stream(maps).forEach(t -> t.begin(readWrite));
        store.begin(readWrite); // should this be write
        uris.begin(readWrite); // should this be write?
    }

    private void commitF() {
        Arrays.stream(maps).forEach(t -> t.commit());
        store.commit(); // should this be write
        uris.commit(); // should this be write?
    }

    private void abortF() {
        Arrays.stream(maps).forEach(t -> t.abort());
        store.abort(); // should this be write
        uris.abort(); // should this be write?
    }

    private void endF() {
        Arrays.stream(maps).forEach(t -> t.end());
        store.end(); // should this be write
        uris.end(); // should this be write?
    }

    @Override
    public void close() {
        if (isInTransaction())
            abort();
    }

    // ** STANDARD CODE

    public long register(Quad quad) {

        return txnController.doInTxn(WRITE, () -> {
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
        txnController.doInTxn(WRITE, () -> {
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
        return txnController.doInTxn(READ, () -> {
            return store.size();
        });
    }

    private Bitmap merge(Bitmap map, Node n, Idx idx) {
        Bitmap bitmap = null;
        if (n != null) {
            long l = uris.get(n);
            if (l <= Store.NO_INDEX) {
                return bitmap;
            }
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
        return txnController.doInTxn(READ, () -> {
            return Triple.create(uris.get(idx.get(Idx.S)), uris.get(idx.get(Idx.P)), uris.get(idx.get(Idx.O)));
        });

    }

    public Quad asQuad(IdxQuad idx) {
        return txnController.doInTxn(READ, () -> {
            return Quad.create(uris.get(idx.get(Idx.G)), uris.get(idx.get(Idx.S)), uris.get(idx.get(Idx.P)),
                    uris.get(idx.get(Idx.O)));
        });
    }

    public <T> ExtendedIterator<T> find(Quad quad, Function<IdxQuad, T> mapper) {
        return txnController.doInTxn(READ, () -> {
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
            return bitmap == null ? NiceIterator.emptyIterator()
                    : WrappedIterator.create(new IdxQuadIterator(bitmap)).mapWith(mapper);
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

    @Override
    public boolean isInTransaction() {
        return txnController.isInTransaction();
    }

    @Override
    public void begin(TxnType type) {
        txnController.begin(type);
    }

    @Override
    public boolean promote(Promote mode) {
        return txnController.promote(mode);
    }

    @Override
    public void commit() {
        txnController.commit();
    }

    @Override
    public void abort() {
        txnController.abort();
    }

    @Override
    public void end() {
        txnController.end();
    }

    @Override
    public ReadWrite transactionMode() {
        return txnController.transactionMode();
    }

    @Override
    public TxnType transactionType() {
        return txnController.transactionType();
    }

}
