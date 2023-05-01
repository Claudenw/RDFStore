package org.xenei.rdfstore.idx;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.Index;

public abstract class AbstractIndexTest<T> {

    abstract protected Supplier<Index<T>> supplier();

    abstract protected T get();

    @Test
    public void registerTest() {
        Index<T> idx = supplier().get();

        T one = get();
        T two = get();
        T three = get();
        idx.register(one, 1);
        idx.register(two, 2);
        idx.register(three, 3);

        Bitmap bitmap = idx.get(one);
        assertEquals(1 << 1, bitmap.firstEntry().bitmap());

        bitmap = idx.get(two);
        assertEquals(1 << 2, bitmap.firstEntry().bitmap());

        bitmap = idx.get(three);
        assertEquals(1 << 3, bitmap.firstEntry().bitmap());
    }

    @Test
    public void deleteTest() {
        Index<T> idx = supplier().get();

        T one = get();
        T two = get();
        T three = get();
        idx.register(one, 1);
        idx.register(two, 2);
        idx.register(three, 3);

        idx.delete(two, 2);

        assertEquals(1 << 1, idx.get(one).firstEntry().bitmap());
        assertTrue(idx.get(two).isEmpty());
        assertEquals(1 << 3, idx.get(three).firstEntry().bitmap());
    }

    @Test
    public void getTest() {
        Index<T> idx = supplier().get();

        T one = get();
        T two = get();
        T three = get();
        idx.register(one, 1);
        idx.register(three, 3);

        assertEquals(1 << 1, idx.get(one).firstEntry().bitmap());
        assertTrue(idx.get(two).isEmpty());
        assertEquals(1 << 3, idx.get(three).firstEntry().bitmap());
    }

    @Test
    public void sizeTest() {
        Index<T> idx = supplier().get();

        T one = get();
        T two = get();
        T three = get();

        assertEquals(0, idx.size());

        idx.register(one, 1);
        assertEquals(1, idx.size());

        idx.register(two, 2);
        assertEquals(2, idx.size());

        idx.register(three, 3);
        assertEquals(3, idx.size());

        idx.delete(two, 2);
        assertEquals(2, idx.size());

        idx.delete(one, 1);
        assertEquals(1, idx.size());

        idx.delete(three, 3);
        assertEquals(0, idx.size());
    }

}
