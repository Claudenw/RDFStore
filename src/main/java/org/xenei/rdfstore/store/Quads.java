package org.xenei.rdfstore.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.xenei.rdfstore.FixedSizeGatedList;
import org.xenei.rdfstore.LargePagedStore;
import org.xenei.rdfstore.LongList;
import org.xenei.rdfstore.Store;
import org.xenei.rdfstore.TrieStore;
import org.xenei.rdfstore.idx.Bitmap;

public class Quads {
    private final URIs uris;
    private final LargePagedStore<ByteBuffer> store;
    private final LongList<Bitmap>[] maps;

    private static Hasher hashByteBuffer(ByteBuffer buffer) {
        long[] hash;
        if (buffer.hasArray()) {
            hash = MurmurHash3.hash128x64( buffer.array());
        } else {
            byte[] buff = new byte[buffer.capacity()];
            int pos = buffer.position();
            buffer.position(0);
            buffer.get(buff);
            buffer.position(pos);
            hash = MurmurHash3.hash128x64( buff);
        }
        return new EnhancedDoubleHasher(hash[0], hash[1]);
    }
    
    public Quads() {
        uris = new URIs();
        int maxPageSize = Store.Page.calculatePageSize(Bitmap.MAX_INDEX);
        store = new LargePagedStore<ByteBuffer>( maxPageSize, Quads::hashByteBuffer );
        maps =  new LongList[Idx.values().length] ;
        
        for (Idx idx : Idx.values())
        {
            maps[idx.ordinal()] = new LongList<Bitmap>();
        }
        
       /* pageSize = 10000;
        lists = new ArrayList<>();
        this.shape = Shape.fromNP(pageSize, 1.0 / 20000);
        lists.add(new FixedSizeGatedList<>(shape, pageSize));
        */
    }

    private class IdxQuad {
        ByteBuffer buffer;
        
        IdxQuad(Quad quad) {
            buffer = ByteBuffer.allocate( Long.BYTES * 4);
            buffer.putLong( uris.register( Idx.G.from(quad)));
            buffer.putLong( uris.register( Idx.S.from(quad)));
            buffer.putLong( uris.register( Idx.P.from(quad)));
            buffer.putLong( uris.register( Idx.O.from(quad)));
        }
        
        IdxQuad(ByteBuffer buffer) {
            this.buffer=buffer;
        }
        
        long get( Idx idx) {
            return buffer.getLong( idx.bufferPos);
        }
        
        ByteBuffer getBuffer( Idx idx ) {
            ByteBuffer result = buffer.position( idx.bufferPos ).slice();
            result.limit( Long.BYTES );
            return result;
        }
    }
    
    public long register(Triple triple) {
        return register(Quad.create(Quad.defaultGraphNodeGenerated, triple));
    }

    public long register(Quad quad) {
        if (quad.isTriple()) {
            return register(quad.asTriple());
        }

        IdxQuad idxQ = new IdxQuad( quad );
        Store.Result result = store.register(idxQ.buffer);
        if (!result.existed) {
            for (Idx idx : Idx.values())
            {
                Bitmap bitmap = new Bitmap();
                bitmap.set( result.value );
                maps[idx.ordinal()].set(idxQ.get(idx), bitmap );
            }
        }
        
        return result.value;
    }
    
    public void delete(Triple triple) {
        delete(Quad.create(Quad.defaultGraphNodeGenerated, triple));
    }
    
    public void delete(Quad quad) {
        IdxQuad idxQ = new IdxQuad( quad );
        Store.Result result = store.delete( idxQ.buffer );
        if (result.existed) {
            for (Idx idx : Idx.values())
            {
                maps[idx.ordinal()].remove(idxQ.get(idx));
            }
        }
    }

    public long size() {
        return store.size();
    }
}
