package org.xenei.rdfstore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.rdfstore.idx.Bitmap;
import org.xenei.rdfstore.pages.BloomFilterPage;

/**
 * An n implementation of a paged store that uses a list to track the pages.
 * Uses Bloom filters on the pages to determine the absence of items. Maximum
 * Total storage is Integer.MAX_VALUE * Store.MAX_INDEX
 *
 * @param <T> The item type being stored.
 */
public class ListPagedStore<T> implements Store<T> {
    /**
     * The maximum number of items that can be stored in the system.
     */
    public static final long MAX_STORAGE = Long.MAX_VALUE;
    /**
     * The maximum page size for this implementation.
     */
    public static final long MAX_PAGESIZE = Bitmap.MAX_INDEX;
    /**
     * A list of pages.
     */
    private final List<BloomFilterPage<T>> pages;
    /**
     * The maximum page size
     */
    private final long maxPageSize;
    /**
     * A Bitmap of indicating the pages that have space on them.
     */
    private final Bitmap hasSpace;
    /**
     * A function to convert object of type T into a Hasher of the object.
     */
    private final Function<T, Hasher> hasherFunc;

    /**
     * 
     * @param maxPageSize the maximum page size for the store. Must be less than
     * Integer.MAX_VALUE ^ 2
     * @param hasherFunc the function to hash
     */
    public ListPagedStore(long maxPageSize, Function<T, Hasher> hasherFunc) {
        if (maxPageSize > MAX_PAGESIZE) {
            throw new IllegalArgumentException("maxPageSize may not be larger than " + MAX_PAGESIZE);
        }
        this.pages = new ArrayList<BloomFilterPage<T>>();
        this.maxPageSize = maxPageSize;
        this.hasSpace = new Bitmap();
        this.hasherFunc = hasherFunc;
    }

    @Override
    public Result register(T item) {
        BloomFilterPage<T> page = null;
        long pageNo = hasSpace.lowest();
        if (pageNo == -1) {
            page = new BloomFilterPage<T>(new TrieStore<T>(), maxPageSize, hasherFunc);
            pages.add(page);
            pageNo = pages.size() - 1;
            hasSpace.set(pageNo);
        } else {
            page = pages.get((int) pageNo);
        }

        BloomFilter filter = page.createFilter(item);
        Result result = page.register(item, filter);
        if (!page.hasSpace()) {
            hasSpace.clear(pageNo);
        }
        return new Result(result.existed, convertPosition(result.index, pageNo));
    }

    private long convertPosition(long offset, long pageNo) {
        return (maxPageSize * pageNo) + offset;
    }

    private long extractPageNo(long position) {
        return position / maxPageSize;
    }

    private long extractOffset(long position) {
        return position % maxPageSize;
    }

    @Override
    public Result delete(T item) {
        BloomFilter target = BloomFilterPage.createFilter(hasherFunc.apply(item), maxPageSize);
        for (int pageNo = 0; pageNo < pages.size(); pageNo++) {
            BloomFilterPage<T> page = pages.get(pageNo);
            Result result = page.delete(item, target);
            if (result.existed) {
                hasSpace.set(pageNo);
                return new Result(true, convertPosition(result.index, pageNo));
            }
        }
        return Store.NO_RESULT;
    }

    @Override
    public boolean contains(T item) {
        BloomFilter target = BloomFilterPage.createFilter(hasherFunc.apply(item), maxPageSize);
        for (int pageNo = 0; pageNo < pages.size(); pageNo++) {
            BloomFilterPage<T> page = pages.get(pageNo);
            if (page.contains(item, target)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long size() {
        if (hasSpace.isEmpty()) {
            return pages.size() * maxPageSize;
        }
        long result = 0;
        for (int pageNo = 0; pageNo < pages.size(); pageNo++) {
            result += (hasSpace.contains(pageNo)) ? pages.get(pageNo).size() : maxPageSize;
        }
        return result;
    }

    @Override
    public Iterator<IdxData<T>> iterator() {
        ExtendedIterator<IdxData<T>> iter = NiceIterator.emptyIterator();
        for (int pageNo = 0; pageNo < pages.size(); pageNo++) {
            final int pgn = pageNo;
            ExtendedIterator<IdxData<T>> inner = WrappedIterator.create(pages.get(pageNo).iterator());
            inner = inner.mapWith(idxData -> new IdxData<T>(convertPosition(idxData.idx, pgn), idxData.data));
            iter.andThen(inner);
        }
        return iter;
    }

    @Override
    public T get(long idx) {
        long pageNo = extractPageNo(idx);
        if (pageNo > pages.size()) {
            return null;
        }
        BloomFilterPage<T> page = pages.get((int) pageNo);
        long offset = extractOffset(idx);
        if (offset > page.size()) {
            return null;
        }
        return page.get(offset);
    }

}
