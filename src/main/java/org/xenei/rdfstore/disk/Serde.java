package org.xenei.rdfstore.disk;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public interface Serde<T> {
    ByteBuffer serialize(T item);

    T deserialize(RandomAccessFile file);

}
