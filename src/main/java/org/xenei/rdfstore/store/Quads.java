package org.xenei.rdfstore.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.xenei.rdfstore.FixedSizeGatedList;
import org.xenei.rdfstore.GatedList;
import org.xenei.rdfstore.idx.Bitmap;

public class Quads {
    private final Bitmap deleted;
    private final List<FixedSizeGatedList<Quad>> lists;
    private final Shape shape;
    private final int pageSize;

    public Quads() {
        deleted = new Bitmap();
        pageSize = 10000;
        lists = new ArrayList<>();
        this.shape = Shape.fromNP(pageSize, 1.0 / 20000);
        lists.add(new FixedSizeGatedList<>(shape, pageSize));
    }

    public GatedList.Result register(Triple triple) {
        return register(Quad.create(Quad.defaultGraphNodeGenerated, triple));
    }

    public GatedList.Result register(Quad quad) {
        if (quad.isTriple()) {
            return register(quad.asTriple());
        }

        BloomFilter bf = new SimpleBloomFilter(shape);
        bf.merge(FixedSizeGatedList.createHasher(quad));

        for (FixedSizeGatedList<Quad> sfgl : lists) {
            GatedList.Result result = sfgl.contains(bf, quad);
            if (result.existed) {
                return result;
            }
        }

        long use = deleted.lowest();
        if (use >= 0) {
            deleted.clear(use);
            FixedSizeGatedList<Quad> fsgl = lists.get((int) (use / pageSize));
            return fsgl.put((int) (use % pageSize), bf, quad);
        }
        // no deleted entries available
        FixedSizeGatedList<Quad> sfgl = lists.get(lists.size() - 1);
        if (!sfgl.hasSpace()) {
            sfgl = new FixedSizeGatedList<Quad>(shape, pageSize);
            lists.add(sfgl);
        }
        return sfgl.register(bf, quad);
    }

    public GatedList.Result remove(Triple triple) {
        return remove(Quad.create(Quad.defaultGraphNodeGenerated, triple));
    }

    public GatedList.Result remove(Quad quad) {
        if (quad.isTriple()) {
            return remove(quad.asTriple());
        }

        BloomFilter bf = new SimpleBloomFilter(shape);
        bf.merge(FixedSizeGatedList.createHasher(quad));

        for (FixedSizeGatedList<Quad> sfgl : lists) {
            GatedList.Result result = sfgl.contains(bf, quad);
            if (result.existed) {
                deleted.set((int) result.value);
                sfgl.remove((int) (result.value % pageSize));
                return result;
            }
        }
        return new GatedList.Result(false, -1);

    }

    public Iterator<Quad> iterator(PrimitiveIterator.OfLong longIter) {
        return new Iterator<Quad>() {

            @Override
            public boolean hasNext() {
                return longIter.hasNext();
            }

            @Override
            public Quad next() {
                long pos = longIter.nextLong();
                int offset = (int) (pos / pageSize);
                int idx = (int) (pos % pageSize);
                return lists.get(offset).get(idx);
            }

        };
    }

    public int size() {
        long result = (lists.size() - 1 * 10000L) + lists.get(lists.size() - 1).size();
        return result > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) result;
    }
}
