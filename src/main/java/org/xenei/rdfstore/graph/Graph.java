package org.xenei.rdfstore.graph;

import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.xenei.rdfstore.store.Quads;

public class Graph extends GraphBase {

    Quads quads = new Quads();

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        return quads.find(triplePattern);
    }

    @Override
    protected int graphBaseSize() {
        long size = quads.size();
        return quads.size() > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    /**
     * Add a triple to the triple store. The default implementation throws an
     * AddDeniedException; subclasses must override if they want to be able to add
     * triples.
     */
    @Override
    public void performAdd(Triple t) {
        quads.register(t);
    }

    /**
     * Remove a triple from the triple store. The default implementation throws a
     * DeleteDeniedException; subclasses must override if they want to be able to
     * remove triples.
     */
    @Override
    public void performDelete(Triple t) {
        quads.delete(t);
    }

}
