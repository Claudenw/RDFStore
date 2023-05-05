package org.xenei.rdfstore.disk;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.apache.commons.jcs3.JCS;
import org.apache.commons.jcs3.access.CacheAccess;
import org.xenei.rdfstore.store.Bitmap;

/**
 * Class to handle a large number of bitmaps.
 */
public class DiskBitmap implements Bitmap {

    private final File name;
    private final CacheAccess<Integer, PageData> cache;

    private static final int PAGE_DATA_SIZE = 16 * 1024 * 1024;
    private static final int PAGE_BITMAP_COUNT = PAGE_DATA_SIZE / Long.BYTES;
    private static final int INDEX_SIZE = PAGE_BITMAP_COUNT / Long.SIZE;

    public DiskBitmap(String fileName) {
        this.name = new File(fileName);
        if (name.exists() && !name.isDirectory()) {
            throw new IllegalArgumentException(fileName + " must be directory or not exist");
        }
        cache = JCS.getInstance("default");
    }

    private int pageNumber(Key key) {
        return (int) (key.asUnsigned() / PAGE_BITMAP_COUNT);
    }

    private PageData getPage(Key key) {
        return getPage(pageNumber(key), this::getPageFromDisk);
    }

    private PageData getPage(int pageNumber, Function<Integer, PageData> creator) {
        PageData result = cache.get(pageNumber);
        if (result == null) {
            result = creator.apply(pageNumber);
            if (result != null) {
                cache.put(pageNumber, result);
            }
        }
        return result;
    }

    private PageData getPageFromDisk(Integer pageNumber) {
        File f = new File(name, pageNumber.toString());
        try {
            return f.exists() ? new PageData(f, pageNumber) : null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private TreeMap<Integer, File> sortedFiles() {
        TreeMap<Integer, File> result = new TreeMap<>();
        for (File f : name.listFiles()) {
            try {
                result.put(Integer.parseInt(f.getName()), f);
            } catch (NumberFormatException e) {
                // do nothing
            }
        }
        return result;
    }

    @Override
    public Key firstIndex() {
        Map.Entry<Integer, File> entry = sortedFiles().firstEntry();
        if (entry == null) {
            return null;
        }
        PageData pageData = getPage(entry.getKey().intValue(), this::getPageFromDisk);
        return pageData.firstIndex();
    }

    public Key lastIndex() {
        Map.Entry<Integer, File> entry = sortedFiles().lastEntry();
        if (entry == null) {
            return null;
        }
        PageData pageData = getPage(entry.getKey().intValue(), this::getPageFromDisk);
        return pageData.lastIndex();
    }

    @Override
    public Key higherIndex(Key key) {
        if (key == null) {
            throw new IllegalArgumentException("key may not be null");
        }
        PageData pageData = getPage(key);
        Key result = pageData.higherIndex(key);
        if (result == null) {
            Integer pageKey = sortedFiles().higherKey(pageData.pageNumber);
            if (pageKey != null) {
                pageData = getPage(pageKey.intValue(), this::getPageFromDisk);
                return pageData.firstIndex();
            }
        }
        return result;
    }

    @Override
    public Entry get(Key key) {
        if (key == null) {
            return null;
        }
        PageData pageData = getPage(key);
        return pageData == null ? null : pageData.get(key);
    }

    @Override
    public Entry firstEntry() {
        return get(firstIndex());
    }

    @Override
    public Entry lastEntry() {
        return get(lastIndex());
    }

    @Override
    public Iterator<? extends Bitmap.Entry> entries() {
        return new Iter();
    }

    @Override
    public Entry put(Key key, Bitmap.Entry entry) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        return getPage(pageNumber(key), this::createPageOnDisk).put(key, entry);

    }

    @Override
    public void clear() {
        for (File f : name.listFiles()) {
            f.delete();
            cache.clear();
        }
    }

    @Override
    public void remove(Key key) {
        if (key != null) {
            getPage(key).remove(key);
        }
    }

    @Override
    public boolean isEmpty() {
        return sortedFiles().isEmpty();
    }

    @Override
    public long pageCount() {
        return sortedFiles().size();
    }

    @SuppressWarnings("unused")
    private PageData createPageOnDisk(Integer pageNumber) {
        File file = new File(name, pageNumber.toString());
        try {
            if (!file.exists()) {

                file.createNewFile();
                try (FileOutputStream fos = new FileOutputStream(file)) {
                    byte[] nulls = IOUtils.byteArray(PAGE_SIZE + INDEX_SIZE);
                    Arrays.fill(nulls, (byte) 0);
                    fos.write(nulls);
                }
            }
            return new PageData(file, pageNumber);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class Iter implements Iterator<Entry> {
        final TreeMap<Integer, File> files;
        Integer key;
        PageData pageData;
        Iterator<Entry> entryIterator;

        Iter() {
            files = sortedFiles();
            key = files.firstKey();
            if (loadPage()) {
                entryIterator = pageData.iterator();
                if (!entryIterator.hasNext()) {
                    nextIterator();
                }
            }
        }

        private boolean checkKey() {
            if (key == null) {
                pageData = null;
                return false;
            }
            return true;
        }

        private boolean loadPage() {
            if (!checkKey()) {
                return false;
            }
            pageData = getPage(key.intValue(), DiskBitmap.this::getPageFromDisk);
            while (pageData == null) {
                key = files.higherKey(key);
                if (!checkKey()) {
                    return false;
                }
                pageData = getPage(key.intValue(), DiskBitmap.this::getPageFromDisk);
            }
            return true;
        }

        private boolean nextIterator() {
            if (!checkKey()) {
                entryIterator = null;
                return false;
            }
            key = files.higherKey(key);
            if (loadPage()) {
                entryIterator = pageData.iterator();
                if (!entryIterator.hasNext()) {
                    // rare but possible
                    return nextIterator();
                }
                return true;
            }
            entryIterator = null;
            return false;
        }

        @Override
        public boolean hasNext() {
            if (entryIterator == null || !entryIterator.hasNext()) {
                return nextIterator();
            }
            return true;
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return entryIterator.next();
        }

    }

    private class PageData {
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

//        <T extends Bitmap.Entry> Entry accept(T entry) {
//            if (entry instanceof Entry) {
//                return (Entry) entry;
//            }
//            return new Entry(entry.index(), buffer, pageNumber);
//        }

        private long pageOffset() {
            return pageNumber * PAGE_BITMAP_COUNT;
        }

        /**
         * Returns the position on the page of the Entry containing the key.
         * 
         * @param key the Key to locate
         * @return the position on the page of the Entry containing the key.
         */
        private int pagePosition(Key key) {
            long pagePos = key.asUnsigned() - pageOffset();
            if (pagePos < 0 || pagePos > PAGE_BITMAP_COUNT) {
                throw new IllegalArgumentException(String.format("%s not in range [%s,%s)", key,
                        (PAGE_BITMAP_COUNT * pageNumber), (PAGE_BITMAP_COUNT * (pageNumber + 1))));
            }
            return (int) pagePos;
        }

        /**
         * Gets the first key on this page, or null if it does not exist.
         * 
         * @return the first key on this page, or null if it does not exist.
         */
        Key firstIndex() {
            int pagePosition = BufferBitMap.lowest(index);
            return (pagePosition == NO_INDEX) ? null : new Key(pagePosition + pageOffset());
        }

        /**
         * Gets the first key on this page, or null if it does not exist.
         * 
         * @return the first key on this page, or null if it does not exist.
         */
        Key higherIndex(Key key) {
            int nextKey = BufferBitMap.next(index, key.asSigned());
            return (nextKey == (int) NO_INDEX) ? null : new Key(nextKey);
        }

        /**
         * Gets the last key on this page, or null if it does not exist.
         * 
         * @return the last key on this page, or null if it does not exist.
         */
        Key lastIndex() {
            int pagePosition = BufferBitMap.lowest(index);
            return (pagePosition == NO_INDEX) ? null : new Key(pagePosition + pageOffset());
        }

        Entry get(Key key) {
            int pagePos = pagePosition(key);
            if (!BufferBitMap.contains(index, pagePos)) {
                return null;
            }
            return new Entry(key, buffer, pagePos);
        }

        Entry put(Key key, Bitmap.Entry entry) {
            int pagePos = pagePosition(key);
            buffer.put(pagePos, entry.bitmap());
            BufferBitMap.set(index, pagePos);
            return new Entry(key, buffer, pagePos);
        }

        void remove(Key key) {
            int pagePos = pagePosition(key);
            buffer.put(pagePos, 0L);
            BufferBitMap.set(index, pagePos);
        }

        Iterator<Entry> iterator() {
            return new Iterator<Entry>() {
                Key key = firstIndex();

                @Override
                public boolean hasNext() {
                    return key != null;
                }

                @Override
                public Entry next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    Entry result = get(key);
                    key = higherIndex(key);
                    return result;
                }
            };
        }
    }

    public static class BufferBitMap {
        /** A bit shift to apply to an integer to divided by 64 (2^6). */
        private static final int DIVIDE_BY_64 = 6;

        /** Do not instantiate. */
        private BufferBitMap() {
        }

        /**
         * Calculates the number of bit maps (longs) required for the numberOfBits
         * parameter.
         *
         * <p>
         * <em>If the input is negative the behavior is not defined.</em>
         * </p>
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
         * @param bitMaps The array of bit maps.
         * @param bitIndex the index of the bit to locate.
         * @return {@code true} if the bit is enabled, {@code false} otherwise.
         * @throws IndexOutOfBoundsException if bitIndex specifies a bit not in the
         * range being tracked.
         */
        public static boolean contains(final LongBuffer bitMaps, final int bitIndex) {
            return (bitMaps.get(getLongIndex(bitIndex)) & getLongBit(bitIndex)) != 0;
        }

        public static int lowest(final LongBuffer bitMaps) {
            LongBuffer buf = bitMaps.duplicate();
            buf.position(0);
            long bitmap;
            try {
                while ((bitmap = buf.get()) == 0) {
                }
            } catch (BufferUnderflowException e) {
                return (int) NO_INDEX;
            }
            int offset = buf.position() - 1;
            int result = offset * Long.SIZE;
            while ((bitmap & 0x01L) == 0) {
                bitmap = bitmap >> 1;
                result++;
            }
            return result;
        }

        public static int highest(final LongBuffer bitMaps) {
            LongBuffer buf = bitMaps.duplicate();
            int position = Long.SIZE - 1;
            long bitmap;

            while ((bitmap = buf.get(position)) == 0) {
                position--;
                if (position == (int) NO_INDEX) {
                    return (int) NO_INDEX;
                }
            }

            int result = position * Long.SIZE + 63;
            long mask = 1 << Long.SIZE;
            while ((bitmap & mask) == 0) {
                mask = bitmap >> 1;
                result--;
            }
            return result;
        }

        public static int next(final LongBuffer bitMaps, int bitIndex) {
            int idx = getLongIndex(bitIndex + 1);
            if (idx > bitMaps.limit()) {
                return (int) NO_INDEX;
            }
            LongBuffer buf = bitMaps.duplicate();
            buf.position(idx);
            long bitmap;
            try {
                while ((bitmap = buf.get()) == 0) {
                }
            } catch (BufferUnderflowException e) {
                return (int) NO_INDEX;
            }
            int offset = buf.position() - 1;
            int result = idx == buf.position() ? (idx % Long.SIZE) : 0;
            while ((bitmap & 0x01L) == 0 && result < Long.SIZE) {
                bitmap = bitmap >> 1;
                result++;
            }

            return (result < Long.SIZE) ? offset * Long.SIZE + result : (int) NO_INDEX;
        }

        /**
         * Sets the bit in the bit maps.
         * <p>
         * <em>Does not perform range checking</em>
         * </p>
         *
         * @param bitMaps The array of bit maps.
         * @param bitIndex the index of the bit to set.
         * @throws IndexOutOfBoundsException if bitIndex specifies a bit not in the
         * range being tracked.
         */
        public static void set(final LongBuffer bitMaps, final int bitIndex) {
            int idx = getLongIndex(bitIndex);
            long map = bitMaps.get(idx);
            map |= getLongBit(bitIndex);
            bitMaps.put(idx, map);
        }

        public static void remove(final LongBuffer bitMaps, final int bitIndex) {
            int idx = getLongIndex(bitIndex);
            long map = bitMaps.get(idx);
            map |= ~getLongBit(bitIndex);
            bitMaps.put(idx, map);
        }

        /**
         * Gets the filter index for the specified bit index assuming the filter is
         * using 64-bit longs to store bits starting at index 0.
         *
         * <p>
         * The index is assumed to be positive. For a positive index the result will
         * match {@code bitIndex / 64}.
         * </p>
         *
         * <p>
         * <em>The divide is performed using bit shifts. If the input is negative the
         * behavior is not defined.</em>
         * </p>
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
         * Gets the filter bit mask for the specified bit index assuming the filter is
         * using 64-bit longs to store bits starting at index 0. The returned value is a
         * {@code long} with only 1 bit set.
         *
         * <p>
         * The index is assumed to be positive. For a positive index the result will
         * match {@code 1L << (bitIndex % 64)}.
         * </p>
         *
         * <p>
         * <em>If the input is negative the behavior is not defined.</em>
         * </p>
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

    public static class Entry implements Bitmap.Entry {
        private LongBuffer bitmap;
        private Key key;
        private int offset;

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
        public Bitmap.Entry duplicate() {
            return new Bitmap.DefaultEntry(key, bitmap());
        }

        @Override
        public void mutate(long other, Logical func) {
            bitmap.put(offset, func.apply(bitmap(), other));
        }
    }
}
