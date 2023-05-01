package org.xenei.rdfstore.store;

import static org.apache.jena.query.TxnType.READ;
import static org.apache.jena.query.TxnType.WRITE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xenei.rdfstore.mem.MemBitmap;
import org.xenei.rdfstore.mem.MemQuads;
import org.xenei.rdfstore.txn.TxnController;
import org.xenei.rdfstore.txn.TxnId;

public class AbstractQuads implements Quads {
    private final UriStore uriStore;
    private final Store<ByteBuffer> store;
    private final LongList<Bitmap>[] maps;

    private final static Logger LOG = LoggerFactory.getLogger(MemQuads.class);

    private final TxnController txnController;

    public static class QuadMaps {
        final LongList<Bitmap>[] maps;

        public QuadMaps(LongList<Bitmap> g, LongList<Bitmap> s, LongList<Bitmap> p, LongList<Bitmap> o) {
            maps = new LongList[4];
            maps[Idx.G.ordinal()] = g;
            maps[Idx.S.ordinal()] = s;
            maps[Idx.P.ordinal()] = p;
            maps[Idx.O.ordinal()] = o;
        }

        public LongList<Bitmap> get(Idx idx) {
            return maps[idx.ordinal()];
        }
    }

    @SuppressWarnings("unchecked")
    public AbstractQuads(UriStore uriStore, Store<ByteBuffer> store, QuadMaps maps) {
        TxnId txnId = () -> "Quads";
        this.uriStore = uriStore;
        this.uriStore.setTxnId(txnId);
        this.store = store;
        this.store.setTxnId(txnId);
        this.maps = new LongList[Idx.values().length];

        for (Idx idx : Idx.values()) {
            this.maps[idx.ordinal()] = maps.get(idx);
            this.maps[idx.ordinal()].setTxnId(TxnId.setParent(txnId, () -> "map" + idx.ordinal()));
        }
        txnController = new TxnController(txnId, this::prepareBegin, this::commitF, this::abortF, this::endF);
    }

    private void prepareBegin(ReadWrite readWrite) {
        Arrays.stream(maps).forEach(t -> t.begin(readWrite));
        store.begin(readWrite); // should this be write
        uriStore.begin(readWrite); // should this be write?
    }

    private void commitF() {
        Arrays.stream(maps).forEach(t -> t.commit());
        store.commit(); // should this be write
        uriStore.commit(); // should this be write?
    }

    private void abortF() {
        Arrays.stream(maps).forEach(t -> t.abort());
        store.abort(); // should this be write
        uriStore.abort(); // should this be write?
    }

    private void endF() {
        Arrays.stream(maps).forEach(t -> t.end());
        store.end(); // should this be write
        uriStore.end(); // should this be write?
    }

    @Override
    public void close() {
        if (isInTransaction())
            abort();
    }

    // ** STANDARD CODE

    @Override
    public long register(Quad quad) {

        return txnController.doInTxn(WRITE, () -> {
            if (quad.isTriple()) {
                return register(Quad.create(Quad.defaultGraphNodeGenerated, quad.asTriple()));
            }

            IdxQuad idxQ = new IdxQuad(uriStore, quad);
            Store.Result result = store.register(idxQ.buffer());
            if (!result.existed) {
                for (Idx idx : Idx.values()) {
                    Bitmap bitmap = new MemBitmap();
                    bitmap.set(result.index);
                    maps[idx.ordinal()].set(new IdxData<Bitmap>(idxQ.get(idx), bitmap));
                }
            }

            return result.index;
        });
    }

    @Override
    public void delete(Quad quad) {
        txnController.doInTxn(WRITE, () -> {
            if (quad.isTriple()) {
                delete(Quad.create(Quad.defaultGraphNodeGenerated, quad.asTriple()));
            }
            IdxQuad idxQ = new IdxQuad(uriStore, quad);
            Store.Result result = store.delete(idxQ.buffer());
            if (result.existed) {
                for (Idx idx : Idx.values()) {
                    maps[idx.ordinal()].remove(idxQ.get(idx));
                }
            }
        });
    }

    @Override
    public long size() {
        return txnController.doInTxn(READ, () -> {
            return store.size();
        });
    }

    private Bitmap merge(Bitmap map, Node n, Idx idx) {
        Bitmap bitmap = null;
        if (n != null) {
            long l = uriStore.get(n);
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
        return Bitmap.intersection(() -> new MemBitmap(), map, bitmap);
    }

    @Override
    public Triple asTriple(IdxQuad idx) {
        return txnController.doInTxn(READ, () -> {
            return Triple.create(uriStore.get(idx.get(Idx.S)), uriStore.get(idx.get(Idx.P)),
                    uriStore.get(idx.get(Idx.O)));
        });

    }

    @Override
    public Quad asQuad(IdxQuad idx) {
        return txnController.doInTxn(READ, () -> {
            return Quad.create(uriStore.get(idx.get(Idx.G)), uriStore.get(idx.get(Idx.S)), uriStore.get(idx.get(Idx.P)),
                    uriStore.get(idx.get(Idx.O)));
        });
    }

    @Override
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
                    : WrappedIterator.create(new IdxQuadIterator(this, bitmap)).mapWith(mapper);
        });
    }

    @Override
    public IdxQuad getIdxQuad(long quadId) {
        ByteBuffer bb = store.get(quadId);
        if (bb == null) {
            return null;
        }
        return new IdxQuad(bb);
    }

    @Override
    public Iterator<Node> listNodes(Idx idx) {
        return uriStore.iterator(IdxData.iterator(maps[idx.ordinal()].iterator()));
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
