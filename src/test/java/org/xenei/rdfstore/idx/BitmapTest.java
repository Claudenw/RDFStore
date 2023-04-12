package org.xenei.rdfstore.idx;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Iterator;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.TreeMap;

import org.apache.commons.collections4.bloomfilter.BitMapProducer;
import org.apache.commons.collections4.bloomfilter.IndexProducer;
import org.junit.jupiter.api.Test;
import org.xenei.rdfstore.idx.Bitmap.Entry;

public class BitmapTest {

    private static final long FIRST_INDEX_ON_LAST_PAGE = Long.SIZE * Bitmap.MAX_UNSIGNED_INT; 
    
    @Test
    public void intersectonTest() {
        Bitmap bitmap1 = new Bitmap();
        bitmap1.set( 1 );
        bitmap1.set( 64 );
        bitmap1.set( Bitmap.MAX_INDEX );
        Bitmap bitmap2 = new Bitmap();
        bitmap2.set( 1 );
        bitmap2.set( 65 );
        bitmap2.set( FIRST_INDEX_ON_LAST_PAGE);
        bitmap2.set( Bitmap.MAX_INDEX );
        Bitmap result = Bitmap.intersection( bitmap1, bitmap2 );
        assertTrue( result.contains( 1 ));
        assertFalse( result.contains( 64 ));
        assertFalse( result.contains( FIRST_INDEX_ON_LAST_PAGE));
        assertTrue( result.contains(Bitmap.MAX_INDEX) );
    }
    
    @Test
    public void unionTest() {
        Bitmap bitmap1 = new Bitmap();
        bitmap1.set( 1 );
        bitmap1.set( Bitmap.MAX_INDEX );
        Bitmap bitmap2 = new Bitmap();
        bitmap2.set( FIRST_INDEX_ON_LAST_PAGE);
        bitmap2.set( 64 );

        Bitmap result = Bitmap.union( bitmap1, bitmap2 );
        assertTrue( result.contains( 1 ));
        assertTrue( result.contains( 64 ));
        assertTrue( result.contains(FIRST_INDEX_ON_LAST_PAGE) );
        assertTrue( result.contains(Bitmap.MAX_INDEX) );
        
        bitmap1.set(64);
        result = Bitmap.union( bitmap1, bitmap2 );
        assertTrue( result.contains( 1 ));
        assertTrue( result.contains( 64 ));
        assertTrue( result.contains(FIRST_INDEX_ON_LAST_PAGE) );
        assertTrue( result.contains(Bitmap.MAX_INDEX) );
        }
    
    
    @Test
    public void getLongIndexTest() {
        fail( "not implemented");
        }
    
    @Test
    public void getLongBitTest() {
        fail( "not implemented");
        }
    
    @Test
    public void containsTest() {
        Bitmap bitmap = new Bitmap();
        bitmap.set( 1 );
        bitmap.set( 64 );
        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap.set( Bitmap.MAX_INDEX );
        assertTrue( bitmap.contains( 1 ));
        assertFalse( bitmap.contains( 2 ));
        assertTrue( bitmap.contains( 64 ));
        assertTrue( bitmap.contains(FIRST_INDEX_ON_LAST_PAGE) );
        assertTrue( bitmap.contains(Bitmap.MAX_INDEX) );
    }
    
    @Test
    public void lowestTest() {
        Bitmap bitmap = new Bitmap();
        bitmap.set( 1 );
        bitmap.set( 64 );
        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap.set( Bitmap.MAX_INDEX );
        assertEquals( 1, bitmap.lowest());
        bitmap.clear(bitmap.lowest());
        assertEquals( 64, bitmap.lowest());
        bitmap.clear(bitmap.lowest());
        assertEquals(FIRST_INDEX_ON_LAST_PAGE, bitmap.lowest());
        bitmap.clear(bitmap.lowest());
        assertEquals( Bitmap.MAX_INDEX, bitmap.lowest());
        bitmap.clear(bitmap.lowest());
        assertEquals( -1, bitmap.lowest());
    }
    
    @Test
    public void setTest() {
        Bitmap bitmap = new Bitmap();
        bitmap.set( 1 );
        Map.Entry<Integer,Bitmap.Entry> mapEntry = ( bitmap.entries.firstEntry());
        assertEquals( Integer.valueOf(0), mapEntry.getKey());
        assertEquals( 0x2L, mapEntry.getValue().bitMap);
        assertEquals( mapEntry.getKey(), mapEntry.getValue().index);
        bitmap.set( 64 );
        assertEquals( 2, bitmap.entries.size() );
        mapEntry = ( bitmap.entries.lastEntry());
        assertEquals( Integer.valueOf(1), mapEntry.getKey());
        assertEquals( 0x1L, mapEntry.getValue().bitMap);
        assertEquals( mapEntry.getKey(), mapEntry.getValue().index);
        
        // check for MaxInt entries
        
        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        assertEquals( 3, bitmap.entries.size());
        mapEntry = ( bitmap.entries.lastEntry());
        assertEquals( Bitmap.MAX_UNSIGNED_INT, Integer.toUnsignedLong( mapEntry.getKey()));
        assertEquals( 0x1L, mapEntry.getValue().bitMap);
        assertEquals( mapEntry.getKey(), mapEntry.getValue().index);
        
        bitmap.set( Bitmap.MAX_INDEX);
        assertEquals( 3, bitmap.entries.size());
        mapEntry = ( bitmap.entries.lastEntry());
        assertEquals( Bitmap.MAX_UNSIGNED_INT, Integer.toUnsignedLong( mapEntry.getKey()));
        assertEquals( 0x8000000000000001L, mapEntry.getValue().bitMap);
        assertEquals( mapEntry.getKey(), mapEntry.getValue().index);
        
        assertThrows(AssertionError.class, () -> bitmap.set(Bitmap.MAX_INDEX+1));
     }
    
    @Test
    public void clearTest() {
        Bitmap bitmap = new Bitmap();
        bitmap.set( 1 );
        bitmap.set( 64 );
        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap.set( Bitmap.MAX_INDEX );
        assertTrue( bitmap.contains( 1 ));
        assertTrue( bitmap.contains( 64 ));
        assertTrue( bitmap.contains(FIRST_INDEX_ON_LAST_PAGE) );
        assertTrue( bitmap.contains(Bitmap.MAX_INDEX) );
        bitmap.clear( 1 );
        assertFalse( bitmap.contains( 1 ));
        assertTrue( bitmap.contains( 64 ));
        assertTrue( bitmap.contains(FIRST_INDEX_ON_LAST_PAGE) );
        assertTrue( bitmap.contains(Bitmap.MAX_INDEX) );
        assertEquals( 2, bitmap.entries.size());
        bitmap.clear( 64 );
        assertFalse( bitmap.contains( 1 ));
        assertFalse( bitmap.contains( 64 ));
        assertTrue( bitmap.contains(FIRST_INDEX_ON_LAST_PAGE) );
        assertTrue( bitmap.contains(Bitmap.MAX_INDEX) );
        assertEquals( 1, bitmap.entries.size());
        bitmap.clear(FIRST_INDEX_ON_LAST_PAGE);
        assertFalse( bitmap.contains( 1 ));
        assertFalse( bitmap.contains( 64 ));
        assertFalse( bitmap.contains(FIRST_INDEX_ON_LAST_PAGE) );
        assertTrue( bitmap.contains(Bitmap.MAX_INDEX) );
        assertEquals( 1, bitmap.entries.size());
        bitmap.clear( Bitmap.MAX_INDEX );
        assertFalse( bitmap.contains( 1 ));
        assertFalse( bitmap.contains( 64 ));
        assertFalse( bitmap.contains(FIRST_INDEX_ON_LAST_PAGE) );
        assertFalse( bitmap.contains(Bitmap.MAX_INDEX) );
        assertEquals( 0, bitmap.entries.size());
        }
    
    @Test
    public void iteratorTest() {
        Bitmap bitmap = new Bitmap();
        bitmap.set( 1 );
        bitmap.set( 64 );
        bitmap.set(FIRST_INDEX_ON_LAST_PAGE);
        bitmap.set( Bitmap.MAX_INDEX );
        PrimitiveIterator.OfLong iter = bitmap.iterator();
        assertTrue( iter.hasNext() );
        assertEquals( 1L, iter.nextLong() );
        assertTrue( iter.hasNext() );
        assertEquals( 64L, iter.nextLong() );
        assertTrue( iter.hasNext() );        
        assertEquals( FIRST_INDEX_ON_LAST_PAGE, iter.nextLong() );
        assertTrue( iter.hasNext() );
        assertEquals( Bitmap.MAX_INDEX, iter.nextLong() );
        assertFalse( iter.hasNext() );
        }
    
    

    /*
     * public class Bitmap {

    private TreeMap<Integer, Entry> entries = new TreeMap<Integer, Entry>();

    public static Bitmap union(Bitmap... maps) {
        Bitmap result = new Bitmap();
        for (Bitmap map : maps) {
            Integer key = map.entries.firstKey();
            while (key != null) {
                result.entries.get(key).union(map.entries.get(key));
                key = map.entries.higherKey(key);
            }
        }
        return result;
    }

    public static Bitmap intersection(Bitmap... maps) {
        Bitmap result = new Bitmap();
        // check for any null entries
        for (Bitmap m : maps) {
            if (m == null) {
                return result;
            }
        }

        Integer key = maps[0].entries.firstKey();
        for (Bitmap map : maps) {
            Integer testKey = map.entries.firstKey();
            if (key > testKey) {
                key = testKey;
            }
        }
        Integer nextKey = null;
        while (key != null) {
            Entry entry = result.entries.get(key);
            for (Bitmap map : maps) {
                Integer testKey = map.entries.higherKey(key);
                nextKey = nextKey == null ? testKey : (nextKey > testKey ? testKey : nextKey);
                entry.intersection(map.entries.get(key));
            }
            key = nextKey;
            nextKey = null;
        }
        return result;
    }

    public boolean contains(final int bitIndex) {
        Entry entry = entries.get(Integer.valueOf(getLongIndex(bitIndex)));
        return entry == null ? false : entry.contains(bitIndex);
    }

   
    public int lowest() {
        if (entries.isEmpty()) {
            return -1;
        }
        Map.Entry<Integer, Bitmap.Entry> entry = entries.firstEntry();
        long idx = entry.getValue().bitMap;
        int result = 0;
        while ((idx & 0x01L) == 0) {
            idx = idx >> 1;
            result++;
            if (result > 64) {
                throw new IllegalStateException("Bit count too large");
            }
        }
        return entry.getKey().intValue() * 64 + result;
    }

    
    public void set(final int bitIndex) {
        Integer entryIndex = Integer.valueOf(getLongIndex(bitIndex));
        Entry entry = entries.get(entryIndex);
        if (entry == null) {
            entry = new Entry(entryIndex);
            entries.put(entryIndex, entry);
        }

        entry.set(bitIndex);
    }

   
    public void clear(final int bitIndex) {

        Integer entryIdx = Integer.valueOf(getLongIndex(bitIndex));
        Entry entry = entries.get(entryIdx);
        if (entry != null) {
            entry.clear(bitIndex);
            if (entry.isEmpty()) {
                entries.remove(entryIdx);
            }
        }
    }

    public PrimitiveIterator.OfInt iterator() {
        return new Iter();
    }

  
    public static int getLongIndex(final int bitIndex) {
        // An integer divide by 64 is equivalent to a shift of 6 bits if the integer is
        // positive.
        // We do not explicitly check for a negative here. Instead we use a
        // signed shift. Any negative index will produce a negative value
        // by sign-extension and if used as an index into an array it will throw an
        // exception.
        return bitIndex >> DIVIDE_BY_64;
    }

    
    public static long getLongBit(final int bitIndex) {
        // Bit shifts only use the first 6 bits. Thus it is not necessary to mask this
        // using 0x3f (63) or compute bitIndex % 64.
        // Note: If the index is negative the shift will be (64 - (bitIndex & 0x3f)) and
        // this will identify an incorrect bit.
        return 1L << bitIndex;
    }

    

    private class Iter implements PrimitiveIterator.OfInt {
        Iterator<Entry> iterE = entries.values().iterator();
        Entry entry = null;
        PrimitiveIterator.OfInt idxIter = null;

        @Override
        public boolean hasNext() {
            if (idxIter == null || !idxIter.hasNext()) {
                idxIter = nextIdxIter();
            }
            return idxIter.hasNext();
        }

        private PrimitiveIterator.OfInt nextIdxIter() {
            entry = nextEntry();
            return new IdxIter();
        }

        private class IdxIter implements PrimitiveIterator.OfInt {
            final int[] values;
            int pos;
            final int offset;

            IdxIter() {
                if (entry == null) {
                    values = new int[0];
                } else {
                    values = IndexProducer.fromBitMapProducer(BitMapProducer.fromBitMapArray(entry.bitMap))
                            .asIndexArray();
                }
                pos = 0;
                offset = entry.index.intValue();
            }

            @Override
            public boolean hasNext() {
                return pos < values.length;
            }

            @Override
            public int nextInt() {
                return values[pos++] + offset;
            }
        }

        private Entry nextEntry() {
            if (iterE.hasNext()) {
                return iterE.next();
            } 
            return null;
        }

        @Override
        public int nextInt() {
            return idxIter.nextInt();
        }
    }
}

     */
}
