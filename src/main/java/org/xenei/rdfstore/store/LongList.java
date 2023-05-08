package org.xenei.rdfstore.store;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.jena.sparql.core.mem.TransactionalComponent;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.xenei.rdfstore.txn.TxnIdHolder;

/**
 * An implementation of a list like structure that can hold up to Long.MAX_VALUE
 * objects.
 * 
 * @param <T> the type to store.
 */
public interface LongList<T> extends TransactionalComponent, TxnIdHolder {

    /**
     * Adds the data item to the list.
     * 
     * @param data the item to add.
     */
    public IdxData<T> add(T data);

    /**
     * returns the size of the list.
     * 
     * @return the size of the list.
     */
    public long size();

    /**
     * Sets an item at the specified index.
     * 
     * @param idx The index to set the item at
     * @param data the item to place into the list.
     */
    public void set(IdxData<T> data);

    /**
     * Gets the item at the index.
     * 
     * @param idx the index to get the item from.
     * @return the item or {@code null} if no such item exists.
     */
    public T get(long idx);

    /**
     * Removes the item from the list.
     * 
     * @param idx the index of the item to remove.
     */
    public void remove(long idx);

    /**
     * Returns each item in the list once.
     * @return an ExtendedIterator over the items in the list.
     */
    public ExtendedIterator<IdxData<T>> iterator();

    class LongListIterator<T> implements Iterator<IdxData<T>> {
        IdxData<T> next = null;
        Iterator<IdxData<T>> iterT = null;
        Iterator<Set<IdxData<T>>> pageIter;

        public LongListIterator(Iterator<Set<IdxData<T>>> pageIter) {
            this.pageIter = pageIter;
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                next = getNext();
            }
            return next != null;
        }

        private IdxData<T> getNext() {
            if (iterT == null || !iterT.hasNext()) {
                iterT = getIterT();
            }
            return iterT == null ? null : iterT.next();
        }

        private Iterator<IdxData<T>> getIterT() {
            if (pageIter.hasNext()) {
                return pageIter.next().iterator();
            }
            return null;
        }

        @Override
        public IdxData<T> next() {
            if (hasNext()) {
                IdxData<T> result = next;
                next = null;
                return result;
            }
            throw new NoSuchElementException();
        }
    }
}
