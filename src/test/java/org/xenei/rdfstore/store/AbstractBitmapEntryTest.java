package org.xenei.rdfstore.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.xenei.rdfstore.store.Bitmap.Key;

public abstract class AbstractBitmapEntryTest {
    private Key key0 = new Key(0);
    private Key key1 = new Key(1);
    private Key key2 = new Key(2);
    
    abstract protected Bitmap.Entry create(Key key);
    abstract protected Bitmap.Entry create(Key key, long bitmap);
    

    @Test
    public void compareToTest() {
        Bitmap.Entry entry1 = create(key1);
        Bitmap.Entry entry2 = create(key2);

        assertEquals(0, entry1.compareTo(new Bitmap.DefaultEntry(key1)));
        assertEquals(0, entry1.compareTo(entry1));
        assertEquals(-1, entry1.compareTo(entry2));
        assertEquals(1, entry2.compareTo(entry1));
    }

    @Test
    public void containsTest() {
        Bitmap.Entry entry13 = create(key0, 3);
        Bitmap.Entry entry11 = create(key0, 1);
        Bitmap.Entry entry23 = create(key1, 3);

        assertTrue(entry13.contains(0));
        assertTrue(entry13.contains(1));
        assertTrue(entry11.contains(0));
        assertFalse(entry11.contains(1));
        assertTrue(entry23.contains(0));
        assertTrue(entry23.contains(1));
    }

    @Test
    public void setTest() {
        Bitmap.Entry entry = create(key0, 3);
        assertEquals(0x3L, entry.bitmap());
        entry.set(2);
        assertEquals(0x7L, entry.bitmap());
        entry.set(3);
        assertEquals(0xFL, entry.bitmap());
    }

    @Test
    public void clearTest() {
        Bitmap.Entry entry = create(key0, 0xFL);
        entry.clear(2);
        assertEquals(0xBL, entry.bitmap());
        entry.clear(3);
        assertEquals(0x3L, entry.bitmap());
        entry.clear(0);
        assertEquals(0x2L, entry.bitmap());
        entry.clear(1);
        assertEquals(0x0L, entry.bitmap());
    }

    @Test
    public void isEmptyTest() {
        Bitmap.Entry entry = new Bitmap.DefaultEntry(key0, 0xFL);
        entry.clear(2);
        assertFalse(entry.isEmpty());
        entry.clear(3);
        assertFalse(entry.isEmpty());
        entry.clear(0);
        assertFalse(entry.isEmpty());
        entry.clear(1);
        assertTrue(entry.isEmpty());
    }

    @Test
    public void unionTest() {
        Bitmap.Entry entry1 = create(key0, 0xD);
        Bitmap.Entry entry2 = create(key0, 0x3);
        entry1.union(entry2);
        assertEquals(0xFL, entry1.bitmap());

        entry1 = create(key0, 0x4);
        entry1.union(entry2);
        assertEquals(0x7L, entry1.bitmap());

    }

    @Test
    public void intersectionTest() {
        Bitmap.Entry entry1 = create(key0, 0xF);
        Bitmap.Entry entry2 = create(key0, 0x3);
        entry1.intersection(entry2);
        assertEquals(0x3L, entry1.bitmap());

        entry1 = create(key0, 0xD);
        entry1.intersection(entry2);
        assertEquals(0x1L, entry1.bitmap());

        entry1 = create(key0, 0x4);
        entry1.intersection(entry2);
        assertEquals(0x0L, entry1.bitmap());
    }
}
