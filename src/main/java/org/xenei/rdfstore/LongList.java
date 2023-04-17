package org.xenei.rdfstore;

import java.util.ArrayList;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * An implementation of a list like structure that can hold up to Long.MAX_VALUE
 * objects.
 * 
 * @param <T> the type to store.
 */
public class LongList<T> {
    public static final long MAX_ITEM_INDEX = (Integer.MAX_VALUE * (long) Integer.MAX_VALUE) - 1;

    private final ArrayList<NavigableSet<IdxData<T>>> pages;
    private long itemCount;

    /**
     * Creates a LongList.
     */
    public LongList() {
        this.pages = new ArrayList<NavigableSet<IdxData<T>>>();
        pages.add( new TreeSet<IdxData<T>>() );
        this.itemCount = 0;
    }

    private static void checkIndex(long idx) {
        if (idx > MAX_ITEM_INDEX) {
            throw new IllegalArgumentException("Index too large: " + idx + " Maximum value allowed: " + MAX_ITEM_INDEX);
        }
        if (idx < 0) {
            throw new IllegalArgumentException("Index may not be less than zero");
        }
    }

    /**
     * gets the page number for the item index.
     * 
     * @param idx the index to search for.
     * @return the pageNumber.
     */
    private static int getPageNumber(long idx) {
        checkIndex(idx);
        return (int) idx / Integer.MAX_VALUE;
    }

    /**
     * Get the page offset for the item index.
     * 
     * @param idx
     * @return the offset into the page for the item.
     */
    private static int getPageOffset(long idx) {
        return (int) idx % Integer.MAX_VALUE;
    }

    /**
     * Adds the data item to the list.
     * 
     * @param data the item to add.
     */
    public IdxData add(T data) {
        NavigableSet<IdxData<T>> page = pages.get(pages.size() - 1);
        if (page.size() == Integer.MAX_VALUE) {
            page = new TreeSet<IdxData<T>>();
            pages.add(page);
        }
        IdxData idxData = new IdxData(itemCount,data); 
        page.add(idxData);
        itemCount++;
        return idxData;
    }

    /**
     * returns the size of the list.
     * 
     * @return the size of the list.
     */
    public long size() {
        return itemCount;
    }

    /**
     * Sets an item at the specified index.
     * 
     * @param idx The index to set the item at
     * @param data the item to place into the list.
     */
    public void set(IdxData<T> data) {
        int pageNo = getPageNumber(data.idx);
        NavigableSet<IdxData<T>> page = pages.get(pageNo);
        if (page == null) {
            page = new TreeSet<IdxData<T>>();
            pages.add( page );
        }
        if (page.add(data)) {
            if (data.data == null) {
                itemCount--;
            } else {
                itemCount++;
            }
        }
    }

    /**
     * Gets the item at the index.
     * 
     * @param idx the index to get the item from.
     * @return the item or {@code null} if no such item exists.
     */
    public T get(long idx) {
        IdxData searcher = new IdxData( idx, null );
        NavigableSet<IdxData<T>> page = pages.get(getPageNumber(idx));
        IdxData<T> result = page.floor(searcher);
        return result == null?null:(result.idx == searcher.idx) ? result.data : null;
    }

    /**
     * Removes the item from the list.
     * 
     * @param idx the index of the item to remove.
     */
    public void remove(long idx) {
        set(new IdxData(idx, null));
    }

}
