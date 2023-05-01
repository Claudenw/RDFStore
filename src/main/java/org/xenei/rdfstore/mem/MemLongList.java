package org.xenei.rdfstore.mem;

import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;

import java.util.ArrayList;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.rdfstore.store.IdxData;
import org.xenei.rdfstore.store.LongList;
import org.xenei.rdfstore.txn.TxnHandler;
import org.xenei.rdfstore.txn.TxnId;

/**
 * An implementation of a list like structure that can hold up to Long.MAX_VALUE
 * objects.
 * 
 * @param <T> the type to store.
 */
public class MemLongList<T> implements LongList<T> {
    public static final long MAX_ITEM_INDEX = (Integer.MAX_VALUE * (long) Integer.MAX_VALUE) - 1;

    private final TxnHandler txnHandler;
    private final ArrayList<NavigableSet<IdxData<T>>> pages;
    private long itemCount;

    /**
     * Creates a LongList.
     */
    public MemLongList() {
        this.pages = new ArrayList<NavigableSet<IdxData<T>>>();
        pages.add(new TreeSet<IdxData<T>>());
        this.itemCount = 0;
        this.txnHandler = new TxnHandler(() -> "LongList", this::prepareBegin, this::execCommit, this::execAbort,
                this::execEnd);
    }

    @Override
    public void setTxnId(TxnId prefix) {
        txnHandler.setTxnId(prefix);
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

    // ** TRANSACTION FUCNTIONS

    private NavigableSet<IdxData<T>> txnPages;
    private long txnCurrentItem;

    private void prepareBegin(ReadWrite readWrite) {
        txnPages = new TreeSet<IdxData<T>>();
        txnCurrentItem = itemCount;
    }

    private void execCommit() {
        int lastPage = -1;
        NavigableSet<IdxData<T>> page = null;
        for (IdxData<T> data : txnPages) {
            int pageNo = getPageNumber(data.idx);
            if (pageNo != lastPage) {
                page = pages.get(pageNo);
                if (page == null) {
                    page = new TreeSet<IdxData<T>>();
                    pages.add(page);
                }
            }
            lastPage = pageNo;
            if (page.add(data)) {
                if (data.data != null) {
                    itemCount++;
                }
            } else {
                if (data.data == null) {
                    itemCount--;
                }
            }
        }
    }

    private void execAbort() {
        txnPages = null;
        txnCurrentItem = itemCount;
    }

    private void execEnd() {
        txnPages = null;
        txnCurrentItem = itemCount;
    }

    /**
     * Adds the data item to the list.
     * 
     * @param data the item to add.
     */
    @Override
    public IdxData<T> add(T data) {
        return txnHandler.doInTxn(WRITE, () -> {
            IdxData<T> idxData = new IdxData<>(txnCurrentItem++, data);
            txnPages.add(idxData);
            return idxData;
        });
    }

    /**
     * returns the size of the list.
     * 
     * @return the size of the list.
     */
    @Override
    public long size() {
        return txnHandler.doInTxn(ReadWrite.READ, () -> {
            return txnCurrentItem;
        });
    }

    /**
     * Sets an item at the specified index.
     * 
     * @param idx The index to set the item at
     * @param data the item to place into the list.
     */
    @Override
    public void set(IdxData<T> data) {
        txnHandler.doInTxn(WRITE, () -> {
            txnPages.add(data);
        });
    }

    /**
     * Gets the item at the index.
     * 
     * @param idx the index to get the item from.
     * @return the item or {@code null} if no such item exists.
     */
    @Override
    public T get(long idx) {
        return txnHandler.doInTxn(READ, () -> {
            IdxData<T> searcher = new IdxData<>(idx, null);
            IdxData<T> result = txnPages.floor(searcher);
            if (result == null || result.idx != idx) {
                NavigableSet<IdxData<T>> page = pages.get(getPageNumber(idx));
                result = page == null ? null : page.floor(searcher);
                return result == null ? null : (result.idx == idx) ? result.data : null;
            }
            return result.data;
        });
    }

    /**
     * Removes the item from the list.
     * 
     * @param idx the index of the item to remove.
     */
    @Override
    public void remove(long idx) {
        set(new IdxData<>(idx, null));
    }

    @Override
    public ExtendedIterator<IdxData<T>> iterator() {
        return WrappedIterator.create(new LongListIterator(pages.iterator()));
    }

    @Override
    public void begin(ReadWrite readWrite) {
        txnHandler.begin(readWrite);
    }

    @Override
    public void commit() {
        txnHandler.commit();
    }

    @Override
    public void abort() {
        txnHandler.abort();
    }

    @Override
    public void end() {
        txnHandler.end();
    }

}
