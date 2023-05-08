package org.xenei.rdfstore.disk;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.StringUtils;
import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.LongList;

public class LongListOfBitmap extends DiskLongList<DiskBitmap> implements LongList<DiskBitmap> {

    LongListOfBitmap(String fileName) throws IOException {
        super(fileName, new BitmapSerde());
    }

    static class BitmapSerde implements Serde<DiskBitmap> {

        @Override
        public ByteBuffer serialize(DiskBitmap item) {
            byte[] buf = StringUtils.getBytesUtf8(item.getFileName());
            return ByteBuffer.allocate(buf.length + Short.BYTES).putShort((short) buf.length).put(buf).flip();
        }

        @Override
        public DiskBitmap deserialize(RandomAccessFile file) {
            byte[] buf = ExceptionHandler.exH(() -> {
                short s = file.readShort();
                byte[] b = new byte[s];
                file.read(b);
                return b;
            });
            return new DiskBitmap(StringUtils.newStringUtf8(buf));
        }

    }

}
