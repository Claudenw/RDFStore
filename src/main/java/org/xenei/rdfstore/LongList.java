package org.xenei.rdfstore;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.ArrayList;

import org.xenei.rdfstore.idx.Bitmap;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

public class LongList<T> {

    private final ArrayList<ArrayList<T>> pages;
    
    public LongList() {
        this.pages = new ArrayList<ArrayList<T>>();
    }
    
    private static int getPageIdx(long idx) {
        long tmp =  idx / Integer.MAX_VALUE;
        if (tmp > Integer.MAX_VALUE ) {
            throw new IllegalArgumentException( "Value too large: "+idx+" Maximum value allowed: "+Integer.MAX_VALUE);
        }
        return (int)tmp;
    }
    
    private static int getPageOffset(long idx) {
        long tmp = idx % Integer.MAX_VALUE;
        if (tmp > Integer.MAX_VALUE ) {
            throw new IllegalArgumentException( "Value too large: "+idx+" Maximum value allowed: "+Integer.MAX_VALUE);
        }
        return (int)tmp;
    }
    
    public void set(long idx, T data) {
        int pageIdx = getPageIdx( idx );
        int pageOffset = getPageOffset( idx );
        ArrayList<T> page = pages.get( pageIdx );
        if (page == null)
        {
            page = new ArrayList<T>();
            pages.ensureCapacity( pageIdx );
            pages.set(pageIdx, page);
        }
        page.ensureCapacity( pageOffset );
        page.set( pageOffset,  data);
    }
    
    public T get(long idx, T data) {
        ArrayList<T> page = pages.get( getPageIdx( idx ) );
        return page == null?null:page.get(getPageOffset( idx ));
        
    }
    
    public void remove(long idx) {
        set( idx, null );
    }

}
