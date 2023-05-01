package org.xenei.rdfstore.store;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BitMap;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.util.iterator.ExtendedIterator;

public interface Quads extends Transactional, AutoCloseable {

    long register(Quad quad);

    void delete(Quad quad);

    long size();

    Triple asTriple(IdxQuad idx);

    Quad asQuad(IdxQuad idx);

    <T> ExtendedIterator<T> find(Quad quad, Function<IdxQuad, T> mapper);

    Iterator<Node> listNodes(Idx idx);

    IdxQuad getIdxQuad(long quadId);

    class IdxQuadIterator implements Iterator<IdxQuad> {
        private final Quads quads;
        private final PrimitiveIterator.OfLong longIter;
        private IdxQuad next;

        public IdxQuadIterator(Quads quads, Bitmap bitmap) {
            this.quads = quads;
            longIter = bitmap.iterator();
            next = null;
        }

        @Override
        public boolean hasNext() {
            while (next == null && longIter.hasNext()) {
                next = quads.getIdxQuad(longIter.next());
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

    class IdxQuad implements Comparable<IdxQuad> {
        private ByteBuffer buffer;

        public IdxQuad(UriStore uris, Quad quad) {
            buffer = ByteBuffer.allocate(Long.BYTES * 4);
            buffer.putLong(uris.register(Idx.G.from(quad)));
            buffer.putLong(uris.register(Idx.S.from(quad)));
            buffer.putLong(uris.register(Idx.P.from(quad)));
            buffer.putLong(uris.register(Idx.O.from(quad)));
        }

        public IdxQuad(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public ByteBuffer buffer() {
            return buffer.asReadOnlyBuffer();
        }

        public long get(Idx idx) {
            return buffer.getLong(idx.bufferPos);
        }

        public ByteBuffer getBuffer(Idx idx) {
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
