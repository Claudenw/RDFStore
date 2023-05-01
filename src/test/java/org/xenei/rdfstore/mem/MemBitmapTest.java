package org.xenei.rdfstore.mem;

import java.util.function.Supplier;

import org.xenei.rdfstore.store.AbstractBitmapTest;
import org.xenei.rdfstore.store.Bitmap;

public class MemBitmapTest extends AbstractBitmapTest {

    @Override
    protected Supplier<Bitmap> getSupplier() {
        return () -> new MemBitmap();
    }

}
