package org.xenei.rdfstore.graph;

import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;
import org.xenei.rdfstore.GatedList;
import org.xenei.rdfstore.idx.LangIdx;
import org.xenei.rdfstore.idx.NumberIdx;
import org.xenei.rdfstore.store.Quads;
import org.xenei.rdfstore.store.URIs;

public class GraphImpl extends GraphBase {

    Quads quads = new Quads();
    URIs uris = new URIs();
    NumberIdx numbers = new NumberIdx();
    LangIdx lang = new LangIdx();
    // TextIdx text = null;

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        return WrappedIterator.create(quads.iterator(uris.get(triplePattern).iterator())).mapWith(q -> q.asTriple());
    }

    @Override
    protected int graphBaseSize() {
        return quads.size();
    }

    /**
     * Add a triple to the triple store. The default implementation throws an
     * AddDeniedException; subclasses must override if they want to be able to add
     * triples.
     */
    @Override
    public void performAdd(Triple t) {
        GatedList.Result tripleIdx = quads.register(t);
        if (!tripleIdx.existed) {
            uris.register(t, (int) tripleIdx.value);
        }
    }

    /**
     * Remove a triple from the triple store. The default implementation throws a
     * DeleteDeniedException; subclasses must override if they want to be able to
     * remove triples.
     */
    @Override
    public void performDelete(Triple t) {
        GatedList.Result tripleIdx = quads.remove(t);
        if (!tripleIdx.existed) {
            uris.unregister(t, (int) tripleIdx.value);
        }
    }

}
