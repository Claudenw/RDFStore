package org.xenei.rdfstore.disk;

import java.nio.LongBuffer;

import org.xenei.rdfstore.store.AbstractBitmapEntryTest;
import org.xenei.rdfstore.store.Bitmap.Entry;
import org.xenei.rdfstore.store.Bitmap.Key;

public class DiskBitmapEntryTest extends AbstractBitmapEntryTest {

    @Override
    protected Entry create(Key key) {
        // TODO Auto-generated method stub
        return new DiskBitmap.Entry(key, LongBuffer.allocate(1), 0);
    }

    @Override
    protected Entry create(Key key, long bitmap) {
        return new DiskBitmap.Entry(key, LongBuffer.wrap(new long[] { bitmap }), 0);
    }

}
