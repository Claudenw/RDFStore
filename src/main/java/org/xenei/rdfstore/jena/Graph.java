package org.xenei.rdfstore.jena;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.xenei.rdfstore.store.Quads;

public class Graph extends GraphBase {

    Quads quads;
    Node graphName;

    public Graph() {
        this(new Quads());
    }

    public Graph(Quads quads) {
        this(quads, Quad.defaultGraphNodeGenerated);
    }

    public Graph(Quads quads, Node graphName) {
        this.quads = quads;
        this.graphName = graphName;
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {
        return quads.find(Quad.create(graphName, triplePattern), quads::asTriple);
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
        quads.register(Quad.create(graphName, t));
    }

    /**
     * Remove a triple from the triple store. The default implementation throws a
     * DeleteDeniedException; subclasses must override if they want to be able to
     * remove triples.
     */
    @Override
    public void performDelete(Triple t) {
        quads.delete(Quad.create(graphName, t));
    }

}
