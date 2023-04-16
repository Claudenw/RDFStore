package org.xenei.rdfstore;

import java.util.ArrayList;

public class LongList<T> {

    private final ArrayList<ArrayList<T>> pages;
    private long itemCount;

    public LongList() {
        this.pages = new ArrayList<ArrayList<T>>();
        this.itemCount = 0;
    }

    private static int getPageIdx(long idx) {
        long tmp = idx / Integer.MAX_VALUE;
        if (tmp > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Value too large: " + idx + " Maximum value allowed: " + Integer.MAX_VALUE);
        }
        return (int) tmp;
    }

    private static int getPageOffset(long idx) {
        long tmp = idx % Integer.MAX_VALUE;
        if (tmp > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Value too large: " + idx + " Maximum value allowed: " + Integer.MAX_VALUE);
        }
        return (int) tmp;
    }

    public void add(T data) {
        ArrayList<T> page = pages.get(pages.size() - 1);
        if (page.size() == Integer.MAX_VALUE) {
            page = new ArrayList<T>();
            pages.add(page);
        }
        page.add(data);
        itemCount++;
    }

    public long size() {
        return itemCount;
    }

    public void set(long idx, T data) {
        int pageIdx = getPageIdx(idx);
        int pageOffset = getPageOffset(idx);
        ArrayList<T> page = pages.get(pageIdx);
        if (page == null) {
            page = new ArrayList<T>();
            pages.ensureCapacity(pageIdx);
            pages.set(pageIdx, page);
        }
        page.ensureCapacity(pageOffset);
        if (null == page.set(pageOffset, data)) {
            if (data == null) {
                itemCount--;
            } else {
                itemCount++;
            }
        }

    }

    public T get(long idx) {
        ArrayList<T> page = pages.get(getPageIdx(idx));
        return page == null ? null : page.get(getPageOffset(idx));
    }

    public void remove(long idx) {
        set(idx, null);
    }

}
