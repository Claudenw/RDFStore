package org.xenei.rdfstore.idx;


public interface Index<T> {
        
        Bitmap register(T item, int id);
        
        void delete(T item, int id);
        
        Bitmap get(T item);
        
        public int size();
        
}
