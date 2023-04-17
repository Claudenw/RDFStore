package org.xenei.rdfstore.store;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PrimitiveIterator;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.rdfstore.IdxData;
import org.xenei.rdfstore.ListPagedStore;
import org.xenei.rdfstore.LongList;
import org.xenei.rdfstore.Store;
import org.xenei.rdfstore.TrieStore;
import org.xenei.rdfstore.idx.Bitmap;

public class Quads {
    private final URIs uris;
    private final TrieStore<ByteBuffer> store;
    // map of uriIdx to triples.
    private final LongList<Bitmap>[] maps;

    private static Hasher hashByteBuffer(ByteBuffer buffer) {
        long[] hash;
        if (buffer.hasArray()) {
            hash = MurmurHash3.hash128x64(buffer.array());
        } else {
            byte[] buff = new byte[buffer.capacity()];
            int pos = buffer.position();
            buffer.position(0);
            buffer.get(buff);
            buffer.position(pos);
            hash = MurmurHash3.hash128x64(buff);
        }
        return new EnhancedDoubleHasher(hash[0], hash[1]);
    }

    @SuppressWarnings("unchecked")
    public Quads() {
        uris = new URIs();
        store = new TrieStore<ByteBuffer>( ByteBuffer::toString );
        maps = new LongList[Idx.values().length];

        for (Idx idx : Idx.values()) {
            maps[idx.ordinal()] = new LongList<Bitmap>();
        }

    }

    public long register(Triple triple) {
        return register(Quad.create(Quad.defaultGraphNodeGenerated, triple));
    }

    public long register(Quad quad) {
        if (quad.isTriple()) {
            return register(quad.asTriple());
        }

        IdxQuad idxQ = new IdxQuad(uris, quad);
        Store.Result result = store.register(idxQ.buffer);
        if (!result.existed) {
            for (Idx idx : Idx.values()) {
                Bitmap bitmap = new Bitmap();
                bitmap.set(result.index);
                maps[idx.ordinal()].set( new IdxData<Bitmap>(idxQ.get(idx), bitmap));
            }
        }

        return result.index;
    }

    public void delete(Triple triple) {
        delete(Quad.create(Quad.defaultGraphNodeGenerated, triple));
    }

    public void delete(Quad quad) {
        IdxQuad idxQ = new IdxQuad(uris, quad);
        Store.Result result = store.delete(idxQ.buffer);
        if (result.existed) {
            for (Idx idx : Idx.values()) {
                maps[idx.ordinal()].remove(idxQ.get(idx));
            }
        }
    }

    public long size() {
        return store.size();
    }

    public ExtendedIterator<Triple> find(Triple triplePattern) {
        return find(Quad.create(Quad.defaultGraphNodeGenerated, triplePattern));
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

    public ExtendedIterator<Triple> find(Quad quadPattern) {
        Bitmap bitmap = null;
        for (Idx idx : Idx.values()) {
            Node n = idx.from(quadPattern);
            if (n != null) {
                bitmap = merge(bitmap, n, idx);
            }
        }

        return WrappedIterator.create(new QuadIterator(bitmap));
    }

    private Triple asTriple(long tripleId) {
        ByteBuffer bb = store.get(tripleId);
        if (bb == null) {
            return null;
        }
        IdxQuad idx = new IdxQuad( bb );
        return Triple.create(uris.get(idx.get(Idx.S)), uris.get(idx.get(Idx.P)), uris.get(idx.get(Idx.O)));
    }

    private class QuadIterator implements Iterator<Triple> {

        private PrimitiveIterator.OfLong longIter;
        private Triple next;

        QuadIterator(Bitmap bitmap) {
            longIter = bitmap.iterator();
            next = null;
        }

        @Override
        public boolean hasNext() {
            while (next == null && longIter.hasNext()) {
                next = asTriple(longIter.next());
            }
            return next != null;
        }

        @Override
        public Triple next() {
            Triple result = next;
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
