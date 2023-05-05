package org.xenei.rdfstore.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.PrimitiveIterator;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.xenei.rdfstore.mem.MemBitmap;

public abstract class AbstractBitmapTest {

    private static final long FIRST_INDEX_ON_LAST_PAGE = Long.SIZE * Bitmap.MAX_UNSIGNED_INT;

    abstract protected Supplier<Bitmap> getSupplier();

    @Test
    public void intersectonTest() {
        Bitmap bitmap1 = getSupplier().get();
        bitmap1.set(1);
        bitmap1.set(64);
        bitmap1.set(Bitmap.MAX_INDEX);
        Bitmap bitmap2 = getSupplier().get();
        bitmap2.set(1);
        bitmap2.set(65);
        bitmap2.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap2.set(Bitmap.MAX_INDEX);
        Bitmap result = Bitmap.intersection(() -> new MemBitmap(), bitmap1, bitmap2);
        assertTrue(result.contains(1));
        assertFalse(result.contains(64));
        assertFalse(result.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(result.contains(Bitmap.MAX_INDEX));
    }

    @Test
    public void unionTest() {
        Bitmap bitmap1 = getSupplier().get();
        bitmap1.set(1);
        bitmap1.set(Bitmap.MAX_INDEX);
        Bitmap bitmap2 = getSupplier().get();
        bitmap2.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap2.set(64);

        Bitmap result = Bitmap.union(() -> new MemBitmap(), bitmap1, bitmap2);
        assertTrue(result.contains(1));
        assertTrue(result.contains(64));
        assertTrue(result.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(result.contains(Bitmap.MAX_INDEX));

        bitmap1.set(64);
        result = Bitmap.union(() -> new MemBitmap(), bitmap1, bitmap2);
        assertTrue(result.contains(1));
        assertTrue(result.contains(64));
        assertTrue(result.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(result.contains(Bitmap.MAX_INDEX));
    }

    @Test
    public void getLongIndexTest() {
        assertEquals(Bitmap.MAX_UNSIGNED_INT, Bitmap.getLongIndex(FIRST_INDEX_ON_LAST_PAGE));
        assertEquals(0, Bitmap.getLongIndex(0));
    }

    @Test
    public void getLongBitTest() {
        assertEquals(1L, Bitmap.getLongBit(0));
        assertEquals(1L, Bitmap.getLongBit(FIRST_INDEX_ON_LAST_PAGE));
        assertEquals(0x8000000000000000L, Bitmap.getLongBit(63));
        assertEquals(0x8000000000000000L, Bitmap.getLongBit(Bitmap.MAX_INDEX));
    }

    @Test
    public void containsTest() {
        Bitmap bitmap = getSupplier().get();
        bitmap.set(1);
        bitmap.set(64);
        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap.set(Bitmap.MAX_INDEX);
        assertTrue(bitmap.contains(1));
        assertFalse(bitmap.contains(2));
        assertTrue(bitmap.contains(64));
        assertTrue(bitmap.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(bitmap.contains(Bitmap.MAX_INDEX));
    }

    @Test
    public void lowestTest() {
        Bitmap bitmap = getSupplier().get();
        bitmap.set(1);
        bitmap.set(64);
        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap.set(Bitmap.MAX_INDEX);
        assertEquals(1, bitmap.lowest());
        bitmap.clear(bitmap.lowest());
        assertEquals(64, bitmap.lowest());
        bitmap.clear(bitmap.lowest());
        assertEquals(FIRST_INDEX_ON_LAST_PAGE, bitmap.lowest());
        bitmap.clear(bitmap.lowest());
        assertEquals(Bitmap.MAX_INDEX, bitmap.lowest());
        bitmap.clear(bitmap.lowest());
        assertEquals(-1, bitmap.lowest());
    }

    @Test
    public void setTest() {
        Bitmap bitmap = getSupplier().get();
        bitmap.set(1);
        Bitmap.Entry mapEntry = bitmap.firstEntry();
        assertEquals(Integer.valueOf(0), mapEntry.index());
        assertEquals(0x2L, mapEntry.bitmap());
        bitmap.set(64);
        assertEquals(2, bitmap.pageCount());
        mapEntry = bitmap.lastEntry();
        assertEquals(Integer.valueOf(1), mapEntry.index());
        assertEquals(0x1L, mapEntry.bitmap());

        // check for MaxInt entries

        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        assertEquals(3, bitmap.pageCount());
        mapEntry = bitmap.lastEntry();
        assertEquals(Bitmap.MAX_UNSIGNED_INT, mapEntry.index().asUnsigned());
        assertEquals(0x1L, mapEntry.bitmap());

        bitmap.set(Bitmap.MAX_INDEX);
        assertEquals(3, bitmap.pageCount());
        mapEntry = bitmap.lastEntry();
        assertEquals(Bitmap.MAX_UNSIGNED_INT, mapEntry.index().asUnsigned());
        assertEquals(0x8000000000000001L, mapEntry.bitmap());

        assertThrows(AssertionError.class, () -> bitmap.set(Bitmap.MAX_INDEX + 1));
    }

    @Test
    public void clearTest() {
        Bitmap bitmap = getSupplier().get();
        bitmap.set(1);
        bitmap.set(64);
        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap.set(Bitmap.MAX_INDEX);
        assertTrue(bitmap.contains(1));
        assertTrue(bitmap.contains(64));
        assertTrue(bitmap.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(bitmap.contains(Bitmap.MAX_INDEX));
        bitmap.clear(1);
        assertFalse(bitmap.contains(1));
        assertTrue(bitmap.contains(64));
        assertTrue(bitmap.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(bitmap.contains(Bitmap.MAX_INDEX));
        assertEquals(2, bitmap.pageCount());
        bitmap.clear(64);
        assertFalse(bitmap.contains(1));
        assertFalse(bitmap.contains(64));
        assertTrue(bitmap.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(bitmap.contains(Bitmap.MAX_INDEX));
        assertEquals(1, bitmap.pageCount());
        bitmap.clear(FIRST_INDEX_ON_LAST_PAGE);
        assertFalse(bitmap.contains(1));
        assertFalse(bitmap.contains(64));
        assertFalse(bitmap.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(bitmap.contains(Bitmap.MAX_INDEX));
        assertEquals(1, bitmap.pageCount());
        bitmap.clear(Bitmap.MAX_INDEX);
        assertFalse(bitmap.contains(1));
        assertFalse(bitmap.contains(64));
        assertFalse(bitmap.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertFalse(bitmap.contains(Bitmap.MAX_INDEX));
        assertEquals(0, bitmap.pageCount());
    }

    @Test
    public void iteratorTest() {
        Bitmap bitmap = getSupplier().get();
        bitmap.set(1);
        bitmap.set(64);
        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap.set(Bitmap.MAX_INDEX);
        PrimitiveIterator.OfLong iter = bitmap.iterator();
        assertTrue(iter.hasNext());
        assertEquals(1L, iter.nextLong());
        assertTrue(iter.hasNext());
        assertEquals(64L, iter.nextLong());
        assertTrue(iter.hasNext());
        assertEquals(FIRST_INDEX_ON_LAST_PAGE, iter.nextLong());
        assertTrue(iter.hasNext());
        assertEquals(Bitmap.MAX_INDEX, iter.nextLong());
        assertFalse(iter.hasNext());
    }

    @Test
    public void staticXorTest() {
        Bitmap bitmap1 = getSupplier().get();
        bitmap1.set(1);
        bitmap1.set(64);
        bitmap1.set(Bitmap.MAX_INDEX);

        Bitmap bitmap2 = getSupplier().get();
        bitmap2.set(1);
        bitmap2.set(65);
        bitmap2.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap2.set(Bitmap.MAX_INDEX);

        Bitmap result = Bitmap.xor(() -> new MemBitmap(), bitmap1, bitmap2);
        assertFalse(result.contains(1));
        assertTrue(result.contains(64));
        assertTrue(result.contains(65));
        assertTrue(result.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertFalse(result.contains(Bitmap.MAX_INDEX));

        result = Bitmap.xor(() -> new MemBitmap(), bitmap2, bitmap1);
        assertFalse(result.contains(1));
        assertTrue(result.contains(64));
        assertTrue(result.contains(65));
        assertTrue(result.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertFalse(result.contains(Bitmap.MAX_INDEX));

        // different number of pages
        int page3bit = 64 * 3;
        bitmap1.set(page3bit);
        result = Bitmap.xor(() -> new MemBitmap(), bitmap1, bitmap2);
        assertFalse(result.contains(1));
        assertTrue(result.contains(64));
        assertTrue(result.contains(65));
        assertTrue(result.contains(page3bit));
        assertTrue(result.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertFalse(result.contains(Bitmap.MAX_INDEX));

        result = Bitmap.xor(() -> new MemBitmap(), bitmap2, bitmap1);
        assertFalse(result.contains(1));
        assertTrue(result.contains(64));
        assertTrue(result.contains(65));
        assertTrue(result.contains(page3bit));
        assertTrue(result.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertFalse(result.contains(Bitmap.MAX_INDEX));

        // different ending pages
        bitmap1.clear(Bitmap.MAX_INDEX);
        result = Bitmap.xor(() -> new MemBitmap(), bitmap1, bitmap2);
        assertFalse(result.contains(1));
        assertTrue(result.contains(64));
        assertTrue(result.contains(65));
        assertTrue(result.contains(page3bit));
        assertTrue(result.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(result.contains(Bitmap.MAX_INDEX));

        result = Bitmap.xor(() -> new MemBitmap(), bitmap2, bitmap1);
        assertFalse(result.contains(1));
        assertTrue(result.contains(64));
        assertTrue(result.contains(65));
        assertTrue(result.contains(page3bit));
        assertTrue(result.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(result.contains(Bitmap.MAX_INDEX));

        assertTrue(Bitmap.xor(() -> new MemBitmap(), bitmap1, bitmap1).isEmpty());

    }

    private Bitmap xorTestBitmap1() {
        Bitmap bitmap1 = getSupplier().get();
        bitmap1.set(1);
        bitmap1.set(64);
        bitmap1.set(Bitmap.MAX_INDEX);
        return bitmap1;
    }

    private Bitmap xorTestBitmap2() {
        Bitmap bitmap2 = getSupplier().get();
        bitmap2.set(1);
        bitmap2.set(65);
        bitmap2.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap2.set(Bitmap.MAX_INDEX);
        return bitmap2;
    }

    @Test
    public void xorTest() {
        Bitmap bitmap1 = xorTestBitmap1();
        Bitmap bitmap2 = xorTestBitmap2();

        bitmap1.xor(bitmap2);
        assertFalse(bitmap1.contains(1));
        assertTrue(bitmap1.contains(64));
        assertTrue(bitmap1.contains(65));
        assertTrue(bitmap1.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertFalse(bitmap1.contains(Bitmap.MAX_INDEX));

        bitmap1 = xorTestBitmap1();

        bitmap2.xor(bitmap1);
        assertFalse(bitmap2.contains(1));
        assertTrue(bitmap2.contains(64));
        assertTrue(bitmap2.contains(65));
        assertTrue(bitmap2.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertFalse(bitmap2.contains(Bitmap.MAX_INDEX));

        // different number of pages
        int page3bit = 64 * 3;
        bitmap2 = xorTestBitmap2();
        bitmap1.set(page3bit);
        bitmap1.xor(bitmap2);
        assertFalse(bitmap1.contains(1));
        assertTrue(bitmap1.contains(64));
        assertTrue(bitmap1.contains(65));
        assertTrue(bitmap1.contains(page3bit));
        assertTrue(bitmap1.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertFalse(bitmap1.contains(Bitmap.MAX_INDEX));

        bitmap1 = xorTestBitmap1();
        bitmap1.set(page3bit);
        bitmap2.xor(bitmap1);
        assertFalse(bitmap2.contains(1));
        assertTrue(bitmap2.contains(64));
        assertTrue(bitmap2.contains(65));
        assertTrue(bitmap2.contains(page3bit));
        assertTrue(bitmap2.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertFalse(bitmap2.contains(Bitmap.MAX_INDEX));

        // different ending pages
        bitmap2 = xorTestBitmap2();
        bitmap1.clear(Bitmap.MAX_INDEX);
        bitmap1.xor(bitmap2);
        assertFalse(bitmap1.contains(1));
        assertTrue(bitmap1.contains(64));
        assertTrue(bitmap1.contains(65));
        assertTrue(bitmap1.contains(page3bit));
        assertTrue(bitmap1.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(bitmap1.contains(Bitmap.MAX_INDEX));

        bitmap1 = xorTestBitmap1();
        bitmap1.set(page3bit);
        bitmap1.clear(Bitmap.MAX_INDEX);
        bitmap2.xor(bitmap1);
        assertFalse(bitmap2.contains(1));
        assertTrue(bitmap2.contains(64));
        assertTrue(bitmap2.contains(65));
        assertTrue(bitmap2.contains(page3bit));
        assertTrue(bitmap2.contains(FIRST_INDEX_ON_LAST_PAGE));
        assertTrue(bitmap2.contains(Bitmap.MAX_INDEX));

        bitmap1 = xorTestBitmap1();
        bitmap1.xor(bitmap1);
        assertTrue(bitmap1.isEmpty());
    }
}
