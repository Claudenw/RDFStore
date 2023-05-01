package org.xenei.rdfstore.mem;

import java.nio.ByteBuffer;

import org.xenei.rdfstore.store.AbstractQuads;
import org.xenei.rdfstore.store.Bitmap;

public class MemQuads extends AbstractQuads {

    public MemQuads() {
        super(new MemUriStore(), new TrieStore<ByteBuffer>(ByteBuffer::toString),
                new QuadMaps(new MemLongList<Bitmap>(), new MemLongList<Bitmap>(), new MemLongList<Bitmap>(),
                        new MemLongList<Bitmap>()));

    }

}
