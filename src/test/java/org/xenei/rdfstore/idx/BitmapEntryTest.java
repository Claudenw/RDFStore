package org.xenei.rdfstore.idx;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class BitmapEntryTest {

    @Test
    public void compareToTest() {
        Bitmap.Entry entry1 = new Bitmap.Entry(1);
        Bitmap.Entry entry2 = new Bitmap.Entry(2);

        assertEquals(0, entry1.compareTo(new Bitmap.Entry(1)));
        assertEquals(0, entry1.compareTo(entry1));
        assertEquals(-1, entry1.compareTo(entry2));
        assertEquals(1, entry2.compareTo(entry1));
    }

    @Test
    public void containsTest() {
        Bitmap.Entry entry13 = new Bitmap.Entry(0, 3);
        Bitmap.Entry entry11 = new Bitmap.Entry(0, 1);
        Bitmap.Entry entry23 = new Bitmap.Entry(1, 3);

        assertTrue(entry13.contains(0));
        assertTrue(entry13.contains(1));
        assertTrue(entry11.contains(0));
        assertFalse(entry11.contains(1));
        assertTrue(entry23.contains(0));
        assertTrue(entry23.contains(1));
    }

    @Test
    public void setTest() {
        Bitmap.Entry entry = new Bitmap.Entry(0, 3);
        assertEquals(0x3L, entry.bitMap);
        entry.set(2);
        assertEquals(0x7L, entry.bitMap);
        entry.set(3);
        assertEquals(0xFL, entry.bitMap);
    }

    @Test
    public void clearTest() {
        Bitmap.Entry entry = new Bitmap.Entry(0, 0xFL);
        entry.clear(2);
        assertEquals(0xBL, entry.bitMap);
        entry.clear(3);
        assertEquals(0x3L, entry.bitMap);
        entry.clear(0);
        assertEquals(0x2L, entry.bitMap);
        entry.clear(1);
        assertEquals(0x0L, entry.bitMap);
    }

    @Test
    public void isEmptyTest() {
        Bitmap.Entry entry = new Bitmap.Entry(0, 0xFL);
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
        Bitmap.Entry entry1 = new Bitmap.Entry(0, 0xD);
        Bitmap.Entry entry2 = new Bitmap.Entry(0, 0x3);
        entry1.union(entry2);
        assertEquals(0xFL, entry1.bitMap);

        entry1 = new Bitmap.Entry(0, 0x4);
        entry1.union(entry2);
        assertEquals(0x7L, entry1.bitMap);

    }

    @Test
    public void intersectionTest() {
        Bitmap.Entry entry1 = new Bitmap.Entry(0, 0xF);
        Bitmap.Entry entry2 = new Bitmap.Entry(0, 0x3);
        entry1.intersection(entry2);
        assertEquals(0x3L, entry1.bitMap);

        entry1 = new Bitmap.Entry(0, 0xD);
        entry1.intersection(entry2);
        assertEquals(0x1L, entry1.bitMap);

        entry1 = new Bitmap.Entry(0, 0x4);
        entry1.intersection(entry2);
        assertEquals(0x0L, entry1.bitMap);
    }
}
