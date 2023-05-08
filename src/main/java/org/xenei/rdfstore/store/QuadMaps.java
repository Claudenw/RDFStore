package org.xenei.rdfstore.store;

public class QuadMaps {
    private final LongList<Bitmap>[] maps;

    public QuadMaps(LongList<Bitmap> g, LongList<Bitmap> s, LongList<Bitmap> p, LongList<Bitmap> o) {
        maps = new LongList[4];
        maps[Idx.G.ordinal()] = g;
        maps[Idx.S.ordinal()] = s;
        maps[Idx.P.ordinal()] = p;
        maps[Idx.O.ordinal()] = o;
    }

    public LongList<Bitmap> get(Idx idx) {
        return maps[idx.ordinal()];
    }
}