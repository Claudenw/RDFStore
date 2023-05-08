package org.xenei.rdfstore.store;

import org.xenei.rdfstore.store.Bitmap.Entry;

public class BitmapEntryTest extends AbstractBitmapEntryTest {

    @Override
    protected Entry create(Key key) {
        return new Bitmap.DefaultEntry(key);
    }

    @Override
    protected Entry create(Key key, long bitmap) {
        return new Bitmap.DefaultEntry(key, bitmap);
    }

}
