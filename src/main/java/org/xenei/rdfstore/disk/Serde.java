package org.xenei.rdfstore.disk;

import java.nio.ByteBuffer;

public interface Serde<T> {
    ByteBuffer serialize(T item);

    T deserialize(ByteBuffer buff);

}
