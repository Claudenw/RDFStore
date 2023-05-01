package org.xenei.rdfstore.idx;

import java.util.function.Supplier;

import org.xenei.rdfstore.store.Bitmap;

/**
 * An index of language strings to URI values.
 *
 */
public class LangIdx extends AbstractIndex<String> {

    public LangIdx(Supplier<Bitmap> bitmapSupplier, Mapper<String> map) {
        super(() -> "LangIdx", bitmapSupplier, map);
    }
}