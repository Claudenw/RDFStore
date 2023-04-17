package org.xenei.rdfstore;

/**
 * An immutable node within a Store containing both the item being stored and
 * its index in the store.
 *
 * @param <T> the item being stored.
 */
public class IdxData<T> implements Comparable<IdxData<T>> {
    /**
     * The index of the item being stored.
     */
    public final long idx;
    /**
     * The item being stored.
     */
    public final T data;

    /**
     * Construct the ItemData.
     * 
     * @param idx The index of the item being stored.
     * @param data the item being stored.
     */
    public IdxData(long idx, T data) {
        this.idx = idx;
        this.data = data;
    }

    @Override
    public int compareTo(IdxData<T> other) {
        return Long.compare( idx, other.idx);
    }
}
