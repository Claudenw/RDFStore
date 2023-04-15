package org.xenei.rdfstore;

public class IdxData<T> {
    public final int idx;
    public final T data;
    
    public IdxData(int idx, T data) {
        this.idx=idx;
        this.data=data;
    }
}
