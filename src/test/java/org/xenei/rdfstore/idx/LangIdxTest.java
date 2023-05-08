package org.xenei.rdfstore.idx;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.xenei.rdfstore.mem.MemBitmap;
import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.IdxData;
import org.xenei.rdfstore.store.Index;

public class LangIdxTest extends AbstractIndexTest<String> {
    private Map<String, IdxData<Bitmap>> map = new HashMap<>();
    private Mapper<String> mapper = new MapMapper<String>(
            new HashMap<String, IdxData<Bitmap>>());
    private Supplier<Bitmap> supplier = () -> new MemBitmap();

    @Override
    protected Supplier<Index<String>> supplier() {
        return () -> new LangIdx(supplier, mapper);
    }

    int count = 0;

    @Override
    protected String get() {
        return "String" + count++;
    }

}
