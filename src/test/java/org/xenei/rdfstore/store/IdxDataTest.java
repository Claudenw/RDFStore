package org.xenei.rdfstore.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.PrimitiveIterator;

import org.junit.jupiter.api.Test;

public class IdxDataTest {

    @Test
    public void compareToTest() {
        IdxData<String> one = new IdxData<>(1, "one");
        IdxData<String> two = new IdxData<>(2, "two");
        IdxData<String> uno = new IdxData<>(1, "uno");

        assertEquals(0, one.compareTo(one));
        assertEquals(-1, one.compareTo(two));
        assertEquals(1, two.compareTo(one));
        assertEquals(0, one.compareTo(uno));
    }

    @Test
    public void iteratorTest() {

        IdxData<String>[] values = new IdxData[] { new IdxData<>(1, "one"), new IdxData<>(2, "two"),
                new IdxData<>(Integer.MAX_VALUE, "big"),
                new IdxData<>(Integer.toUnsignedLong(Integer.MIN_VALUE), "bigger"),
                new IdxData<>(Long.MAX_VALUE, "biggest") };

        PrimitiveIterator.OfLong iter = IdxData.iterator(Arrays.asList(values).iterator());

        assertTrue(iter.hasNext());
        assertEquals(1, iter.next());

        assertTrue(iter.hasNext());
        assertEquals(2, iter.next());

        assertTrue(iter.hasNext());
        assertEquals(Integer.MAX_VALUE, iter.next());

        assertTrue(iter.hasNext());
        assertEquals(Integer.toUnsignedLong(Integer.MIN_VALUE), iter.next());

        assertTrue(iter.hasNext());
        assertEquals(Long.MAX_VALUE, iter.next());
    }
}
