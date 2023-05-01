package org.xenei.rdfstore.store;

import java.util.Iterator;

import org.apache.jena.sparql.core.mem.TransactionalComponent;
import org.xenei.rdfstore.txn.TxnIdHolder;

/**
 * Generic store interface.
 * 
 * The store interface provides a mechanism to store objects if they do not
 * already exist.
 *
 * @param <T> the object type to store
 */
public interface Store<T> extends TransactionalComponent, TxnIdHolder {

    /**
     * An result indicating no result.
     */
    public static final Result NO_RESULT = new Result(false, -1);

    public static final long NO_INDEX = Bitmap.NO_INDEX;

    /**
     * Register an item if it is not already in the store.
     * 
     * @param item the item to register
     * @return the Result of the registration.
     */
    Result register(T item);

    /**
     * Delete an item from the store if it exists.
     * 
     * @param item the item to delete
     * @return The result of the deletion or {@code NO_RESULT} if the item did not
     * exist.
     */
    Result delete(T item);

    /**
     * Gets the item from the store by index.
     * 
     * @param idx the index of the item
     * @return The item or {@code null} if it does not exist.
     */
    T get(long idx);

    /**
     * Gets the item from the store by index.
     * 
     * @param value the value of the item
     * @return The the index or -1 if it does not exist.
     */
    long get(T value);

    /**
     * Determines if the item is in the store.
     * 
     * @param item the item to search for.
     * @return true if the item is found
     */
    boolean contains(T item);

    /**
     * Gets the number of items in the store.
     * 
     * @return the number of items in the store.
     */
    public long size();

    /**
     * An iterator over the items in the store.
     * 
     * @return An IdxData object for each item in the store.
     */
    public Iterator<IdxData<T>> iterator();

    /**
     * An interface that defines the Page operations for a paged store.
     *
     * @param <T> the item to store on the page
     * @param <F> the filter used by this page implementation to assist locating
     * items on the page.
     */
    interface Page<T, F> {

        /**
         * Calculate a page size based on a maximum storage size.
         * 
         * @param totalSize the maximum storage size.
         * @return the calculated page size.
         */
        public static int calculatePageSize(long totalSize) {
            double d = Math.ceil(Math.sqrt(totalSize));
            long result = (long) d;
            if (result > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Result is too large: " + result + " > " + Integer.MAX_VALUE);
            }
            return (int) result;
        }

        /**
         * Deletes an item on the page using filter.
         * 
         * @param item the item to delete
         * @param filter the filter to assist in locating the item.
         * @return the Result for the item deleted, or {@code NO_RESULT} if the item was
         * not found.
         * @see Store#NO_RESULT
         */
        Result delete(T item, F filter);

        /**
         * Determines if an item is on the page.
         * 
         * @param item the item to look for.
         * @param filter the filter to assies in locating the item.
         * @return {@code true} if the item is on the page, {@code false} otherwise.
         */
        boolean contains(T item, F filter);

        /**
         * Determines if there is any more space on this page for inserts.
         * 
         * @return {@code true} if there is space on the page, {@code false} otherwise.
         */
        boolean hasSpace();

        /**
         * Register an item if it is not already on the page.
         * 
         * @param item the item to register
         * @param filter the filter to help locate the item on the page.
         * @return the Result of the registration.
         */
        Result register(T item, F filter);

        /**
         * Returns the number of items on the page.
         * 
         * @return the number of items on the page.
         */
        long size();

        /**
         * Gets an item by index.
         * 
         * @param idx the index to locate.
         * @return the item or {@code null} if the item does not exist.
         */
        T get(long idx);

        /**
         * Creates an iterator over the page.
         * 
         * @return an iterator of IdxData for the items on the page.
         */
        Iterator<IdxData<T>> iterator();
    }

    /**
     * The an immutable result for many Store operations.
     *
     */
    public static class Result {
        /**
         * {@code true} if the item existed before the operation, {@code false}
         * otherwise.
         */
        public final boolean existed;
        /**
         * The index of the result or -1 if it did not exist.
         */
        public final long index;

        /**
         * Create a Result.
         * 
         * @param existed {@code true} if the item existed before the operation,
         * {@code false} otherwise.
         * @param index The index of the result or -1 if it did not exist.
         */
        public Result(boolean existed, long index) {
            this.existed = existed;
            this.index = index;
        }

        /**
         * Convenience method to determine if this result indicates an item was added.
         * 
         * @return {@code true} if the item was added during the operation,
         * {@code false} otherwise.
         */
        public boolean wasAdded() {
            return (!existed) && index != NO_RESULT.index;
        }
    }
}
