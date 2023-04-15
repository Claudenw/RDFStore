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

public class LargePagedStore<T> implements Store<T,Store.Result> {

    private final List<StringPage<T>> pages;
    private final int maxPageSize;
    private final Bitmap hasSpace;
    private final Function<T,Hasher> hasherFunc;
    
    public LargePagedStore(int maxPageSize, Function<T,Hasher> hasherFunc) {
        this.pages = new ArrayList<StringPage<T>>();
        this.maxPageSize = maxPageSize;
        this.hasSpace = new Bitmap();
        this.hasherFunc = hasherFunc;
    }
    
    private BloomFilter createFilter(T item) {
        BloomFilter target = new SimpleBloomFilter( Page.calculateShape(maxPageSize));
        target.merge( hasherFunc.apply( item ));
        return target;
    }
    
    @Override
    public Result register(T item) {
        StringPage<T> page = null;
        long pageNo = hasSpace.lowest();
        if (pageNo == -1)
        {
            page = new StringPage<T>(new TrieStore<T>(), maxPageSize, hasherFunc);
            pages.add( page );
            pageNo = pages.size()-1;
            hasSpace.set( pageNo );
        } else {
            page = pages.get((int)pageNo);
        }
        
        BloomFilter filter = createFilter(item);
        Result result = page.register(item, filter);
        if (!page.hasSpace()) {
            hasSpace.clear( pageNo);
        }
        return new Result( result.existed, convertPosition( result.value, pageNo));
    }

    private long convertPosition(long position, long pageNo) {
        return (maxPageSize*pageNo)+position;
    }
    
    @Override
    public Result delete(T item) {
        BloomFilter target = createFilter(item);
        for (int pageNo=0;pageNo<pages.size();pageNo++) {
            StringPage<T> page = pages.get(pageNo);
            Result result = page.delete(item,target);
            if (result.existed)
            {
                hasSpace.set( pageNo );
                return new Result( true, convertPosition(result.value, pageNo) );
            }
        }
        return Store.NO_RESULT;
    }

    @Override
    public boolean contains(T item) {
        BloomFilter target = createFilter(item);
        for (int pageNo=0;pageNo<pages.size();pageNo++) {
            StringPage<T> page = pages.get(pageNo);
            if (page.contains(item,target)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long size() {
        if (hasSpace.isEmpty()) {
            return pages.size() * (long)maxPageSize;
        }
        long result = 0;
        for (int pageNo=0;pageNo<pages.size();pageNo++) {
            result += (hasSpace.contains(pageNo)) ? pages.get(pageNo).size() : maxPageSize;
        }
        return result;
    }

    @Override
    public Iterator<IdxData<T>> iterator() {
        ExtendedIterator<IdxData<T>> iter = WrappedIterator.emptyIterator();
        for (int pageNo=0;pageNo<pages.size();pageNo++)
        {
            final int pgn = pageNo;
            ExtendedIterator<IdxData<T>> inner = WrappedIterator.create( pages.get(pageNo).iterator() );
            inner = inner.mapWith( idxData -> new IdxData<T>(convertPosition(idxData.idx, pgn), idxData.data));
            iter.andThen( inner );
        }
        return iter;
    }

}
