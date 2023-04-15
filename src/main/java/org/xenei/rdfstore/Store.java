package org.xenei.rdfstore;

public interface Store<T,R> {
    
    R register(T item);
    
    R delete(T item);
    
    boolean contains(T item);
    
    public int size();
    

    public static class Result {
        public final boolean existed;
        public final long value;
    
        public Result(boolean existed, long value) {
            this.existed = existed;
            this.value = value;
        }
    }

}
