package org.xenei.rdfstore.idx;

import org.apache.jena.sparql.core.mem.TransactionalComponent;
import org.xenei.rdfstore.txn.TxnId;

/**
 * An index that associates bitmaps with items and enables the turning on or off
 * of specific bits in the associated bitmaps.
 * 
 * @param <T> the type of item in the index.
 */
public interface Index<T> extends TransactionalComponent {

    default void checkIndex(long idx, long maxIndex) {
        if (idx < 0 || idx >= maxIndex) {
            throw new IndexOutOfBoundsException("Index must be between [0," + maxIndex + ")");
        }
    }

    /**
     * Register the item at the specified index.
     * 
     * @param item the item to register
     * @param id the id to register with the item
     * @return the Bitmap that associated with the item that has the bit {@code id}
     * enabled.
     */
    Bitmap register(T item, long id);

    /**
     * Deletes the id from the bitmap associated with the item.
     * 
     * @param item the item.
     * @param id the bit to turn off.
     */
    void delete(T item, long id);

    /**
     * Gets the bitmap for the item
     * 
     * @param item the item to search for.
     * @return the bitmap for the item
     */
    Bitmap get(T item);

    /**
     * Gets the number of items in this index.
     * 
     * @return the number of items in this index.
     */
    long size();

}
