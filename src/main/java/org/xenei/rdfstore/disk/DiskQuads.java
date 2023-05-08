package org.xenei.rdfstore.disk;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.xenei.rdfstore.mem.MemLongList;
import org.xenei.rdfstore.mem.MemUriStore;
import org.xenei.rdfstore.mem.TrieStore;
import org.xenei.rdfstore.store.AbstractQuads;
import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.Idx;
import org.xenei.rdfstore.store.QuadMaps;

public class DiskQuads extends AbstractQuads {

    public DiskQuads(File root) throws IOException {
        super(new MemUriStore(), new TrieStore<ByteBuffer>(ByteBuffer::toString),
                new QuadMaps<DiskBitmap>(new LongListOfBitmap(new File(root,Idx.G.name()).getAbsolutePath()), 
                        new LongListOfBitmap(new File(root,Idx.S.name()).getAbsolutePath()),
                        new LongListOfBitmap(new File(root,Idx.P.name()).getAbsolutePath()),
                        new LongListOfBitmap(new File(root,Idx.O.name()).getAbsolutePath())));

    }

}
