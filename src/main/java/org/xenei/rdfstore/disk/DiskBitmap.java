package org.xenei.rdfstore.disk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.xenei.rdfstore.store.Bitmap;
import org.xenei.rdfstore.store.Bitmap.Entry;

/**
 * Class to handle a large number of bitmaps.
 */
public class DiskBitmap implements Bitmap {

    private final File name;

    private static final int PAGE_DATA_SIZE = 16 * 1024 * 1024;
    private static final int PAGE_BITMAP_COUNT = PAGE_DATA_SIZE / Long.BYTES;
    private static final int INDEX_SIZE = PAGE_BITMAP_COUNT / Long.SIZE;

    public DiskBitmap(String fileName) {
        this.name = new File(fileName);
        if (name.exists() && !name.isDirectory()) {
            throw new IllegalArgumentException(fileName + " must be directory or not exist");
        }
    }
    
    private TreeMap<Integer,File> sortedFiles() {
        TreeMap<Integer,File> result = new TreeMap<>();
        for (File f : name.listFiles()) {
            try {
                result.put( Integer.parseInt( f.getName() ), f );
            } catch (NumberFormatException e) {
                // do nothing
            }
        }
        return result;
    }

    @Override
    public Key firstIndex() {
        Map.Entry<Integer,File> entry = sortedFiles().firstEntry();
        if (entry == null) {
            return null;
        }
        try {
            PageData page = new PageData( entry.getValue(), entry.getKey() );
            return page.firstIndex();
        } catch (IOException e) {
            throw new RuntimeException( e );
        }
    }

    @Override
    public Key higherIndex(Key key) {
        
        return entries.higherKey(key);
    }

    @Override
    public Entry get(Key key) {
        return entries.get(key);
    }

    @Override
    public Entry firstEntry() {
        return entries.firstEntry().getValue();
    }

    @Override
    public Iterator<Entry> entries() {
        return entries.values().iterator();
    }

    @Override
    public void put(Key key, Entry entry) {
        entries.put(key, entry);
    }

    @Override
    public void clear() {
        entries.clear();
    }

    @Override
    public void remove(Key key) {
        entries.remove(key);
    }

    @Override
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public long pageCount() {
        return entries.size();
    }

    @Override
    public Entry lastEntry() {
        return entries.lastEntry().getValue();
    }
    
    private PageData getPage(File dir, int pageNumber) throws IOException {
        File file = new File( dir, Integer.toString(pageNumber));
        if (!file.exists()) {
            file.createNewFile();
            try (FileOutputStream fos = new FileOutputStream(file)) {
                byte[] nulls = IOUtils.byteArray(PAGE_SIZE + INDEX_SIZE);
                Arrays.fill(nulls, (byte) 0);
                fos.write(nulls);
            }
        }
        return new PageData(file, pageNumber);
    }

    private class PageData  {
        RandomAccessFile file;
        LongBuffer buffer;
        LongBuffer index;
        int pageNumber;

        PageData(File file, int pageNumber) throws IOException {
            this.file = new RandomAccessFile(file, "rw");
            FileChannel channel = this.file.getChannel();
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_DATA_SIZE).asLongBuffer();
            index = channel.map(FileChannel.MapMode.READ_WRITE, PAGE_DATA_SIZE, INDEX_SIZE).asLongBuffer();
        }
        
        private long pageOffset() {
            return pageNumber * PAGE_BITMAP_COUNT;
        }
        
        Key firstIndex() {
            int idx = BufferBitMap.lowest( index );
            return new Key(idx+pageOffset());
        }
        
        Entry get(Key key) {
            long offset = key.asUnsigned() - pageOffset();
            if (offset < 0 || offset > PAGE_BITMAP_COUNT) {
                throw new IllegalArgumentException( String.format( "Key %s not in range [%s,%s)", key,
                        (PAGE_BITMAP_COUNT*pageNumber), (PAGE_BITMAP_COUNT*(pageNumber+1))));
            }
            if (!BufferBitMap.contains( index, (int)offset)) {
                return null;
            }
            return new Entry( key, buffer, (int)offset ); 
        }
    }
    
    public static class BufferBitMap {
        /** A bit shift to apply to an integer to divided by 64 (2^6). */
        private static final int DIVIDE_BY_64 = 6;

        /** Do not instantiate. */
        private BufferBitMap() {
        }

        /**
         * Calculates the number of bit maps (longs) required for the numberOfBits parameter.
         *
         * <p><em>If the input is negative the behavior is not defined.</em></p>
         *
         * @param numberOfBits the number of bits to store in the array of bit maps.
         * @return the number of bit maps necessary.
         */
        public static int numberOfBitMaps(final int numberOfBits) {
            return (numberOfBits - 1 >> DIVIDE_BY_64) + 1;
        }

        /**
         * Checks if the specified index bit is enabled in the array of bit maps.
         *
         * If the bit specified by bitIndex is not in the bit map false is returned.
         *
         * @param bitMaps  The array of bit maps.
         * @param bitIndex the index of the bit to locate.
         * @return {@code true} if the bit is enabled, {@code false} otherwise.
         * @throws IndexOutOfBoundsException if bitIndex specifies a bit not in the range being tracked.
         */
        public static boolean contains(final LongBuffer bitMaps, final int bitIndex) {
            return (bitMaps.get(getLongIndex(bitIndex)) & getLongBit(bitIndex)) != 0;
        }
        
        public static int lowest(final LongBuffer bitMaps) {
            
            LongBuffer buf = bitMaps.duplicate();
            buf.position(0);
            long bitmap;
            try {
                while ( (bitmap = buf.get()) == 0) {}
            } catch (BufferUnderflowException e) {
                return (int)NO_INDEX;
            }
            int offset = buf.position()-1;
            int result = offset * 64;
            while ((bitmap & 0x01L) == 0) {
                bitmap = bitmap >> 1;
                result++;
            }
            return result;
        }

        /**
         * Sets the bit in the bit maps.
         * <p><em>Does not perform range checking</em></p>
         *
         * @param bitMaps  The array of bit maps.
         * @param bitIndex the index of the bit to set.
         * @throws IndexOutOfBoundsException if bitIndex specifies a bit not in the range being tracked.
         */
        public static void set(final LongBuffer bitMaps, final int bitIndex) {
            int idx = getLongIndex(bitIndex);
            long map = bitMaps.get(idx);
            map |= getLongBit(bitIndex);
            bitMaps.put(idx, map);
        }

        /**
         * Gets the filter index for the specified bit index assuming the filter is using 64-bit longs
         * to store bits starting at index 0.
         *
         * <p>The index is assumed to be positive. For a positive index the result will match
         * {@code bitIndex / 64}.</p>
         *
         * <p><em>The divide is performed using bit shifts. If the input is negative the behavior
         * is not defined.</em></p>
         *
         * @param bitIndex the bit index (assumed to be positive)
         * @return the index of the bit map in an array of bit maps.
         */
        public static int getLongIndex(final int bitIndex) {
            // An integer divide by 64 is equivalent to a shift of 6 bits if the integer is
            // positive.
            // We do not explicitly check for a negative here. Instead we use a
            // signed shift. Any negative index will produce a negative value
            // by sign-extension and if used as an index into an array it will throw an
            // exception.
            return bitIndex >> DIVIDE_BY_64;
        }

        /**
         * Gets the filter bit mask for the specified bit index assuming the filter is using 64-bit
         * longs to store bits starting at index 0. The returned value is a {@code long} with only
         * 1 bit set.
         *
         * <p>The index is assumed to be positive. For a positive index the result will match
         * {@code 1L << (bitIndex % 64)}.</p>
         *
         * <p><em>If the input is negative the behavior is not defined.</em></p>
         *
         * @param bitIndex the bit index (assumed to be positive)
         * @return the filter bit
         */
        public static long getLongBit(final int bitIndex) {
            // Bit shifts only use the first 6 bits. Thus it is not necessary to mask this
            // using 0x3f (63) or compute bitIndex % 64.
            // Note: If the index is negative the shift will be (64 - (bitIndex & 0x3f)) and
            // this will identify an incorrect bit.
            return 1L << bitIndex;
        }
    }
    
    public class Entry implements Bitmap.Entry {
        LongBuffer bitmap;
        Key key;
        int offset;

        public Entry(Key key, LongBuffer bitmap, int offset) {
            this.bitmap = bitmap;
            this.key = key;
            this.offset = offset;
        }
        
        @Override
        public long bitmap() {
            return bitmap.get(offset);
        }

        @Override
        public Key index() {
            return key;
        }

        @Override
        public Entry duplicate() {
            return new MemBitmap.Entry( key, bitmap() );
        }

        @Override
        public void mutate(long other, Logical func) {
            bitmap.put(offset, func.apply(bitmap(), other));
        }
    }
}
