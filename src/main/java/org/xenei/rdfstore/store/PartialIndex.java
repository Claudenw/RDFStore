package org.xenei.rdfstore.store;

public interface PartialIndex<T> {
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
}
