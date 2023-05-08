package org.xenei.rdfstore.store;

public class QuadMaps<T extends Bitmap> {
    private final LongList<T>[] maps;

    public QuadMaps(LongList<T> g, LongList<T> s, LongList<T> p, LongList<T> o) {
        maps = new LongList[4];
        maps[Idx.G.ordinal()] = g;
        maps[Idx.S.ordinal()] = s;
        maps[Idx.P.ordinal()] = p;
        maps[Idx.O.ordinal()] = o;
    }

    public LongList<T> get(Idx idx) {
        return maps[idx.ordinal()];
    }
}