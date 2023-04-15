package org.xenei.rdfstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.junit.jupiter.api.Test;
import org.xenei.rdfstore.Store.Result;

public class GatedListTest {
    
    @Test
    public void registerTest() {
        GatedList<String> lst = new GatedList<>();
        assertEquals( 1000, lst.maxCount );
        for (int i=0;i<1500;i++) {
            Store.Result r = lst.register( Integer.toString( i ));
            assertFalse( r.existed );
        }
        assertEquals( 2000, lst.maxCount );
        assertEquals( 1500, lst.size());
        assertEquals( Shape.fromNP( 2000, 1.0/2000), lst.gatekeeper.getShape());
        
        for (int i=998;i<1100;i++) {
            Store.Result r = lst.register(  Integer.toString( i ));
            assertTrue( r.existed );
        }
    }
    
    @Test
    public void containsTest() {
        GatedList<String> lst = new GatedList<>();
        assertEquals( 1000, lst.maxCount );
        for (int i=0;i<1500;i++) {
            lst.register(  Integer.toString( i ));
        }
        
        for (int i=0;i<1500;i++) {
            assertTrue( lst.contains(  Integer.toString( i )) );           
        }
        for (int i=1500;i<2002;i++) {
            assertFalse( lst.contains(  Integer.toString( i )) );           
        }

    }
}
