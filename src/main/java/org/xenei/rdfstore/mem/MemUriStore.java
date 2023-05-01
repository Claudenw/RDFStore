package org.xenei.rdfstore.mem;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.jena.graph.Node;
import org.xenei.rdfstore.idx.AbstractIndex;
import org.xenei.rdfstore.store.AbstractUriStore;
import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.IdxData;
import org.xenei.rdfstore.store.UriStore;

public class MemUriStore extends AbstractUriStore {

    // public AbstractUriStore(Store<Node> store, Mapper<BigDecimal> numbers,
    // Mapper<String> languages, Supplier<Bitmap> bitmapSupplier ) {

    public MemUriStore() {
        super(new TrieStore<Node>(UriStore::asString),
                new AbstractIndex.MapMapper<BigDecimal>(new TreeMap<BigDecimal, IdxData<Bitmap>>()),
                new AbstractIndex.MapMapper<String>(new HashMap<String, IdxData<Bitmap>>()), () -> new MemBitmap());
    }
}
