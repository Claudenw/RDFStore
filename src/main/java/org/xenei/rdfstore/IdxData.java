package org.xenei.rdfstore;

public class IdxData<T> {
    public final long idx;
    public final T data;

    public IdxData(long idx, T data) {
        this.idx = idx;
        this.data = data;
    }
}
