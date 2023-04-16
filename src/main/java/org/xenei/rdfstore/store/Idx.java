package org.xenei.rdfstore.store;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;

public enum Idx {
    G(0), S(Long.BYTES), P(2 * Long.BYTES), O(3 * Long.BYTES);

    int bufferPos;

    Idx(int bufferPos) {
        this.bufferPos = bufferPos;
    }

    /**
     * returns the proper node from the triple
     * 
     * @param t the triple to get node from.
     * @return the node or null (not Node.ANY)
     */
    public Node from(Triple t) {
        switch (this) {
        case G:
            return Quad.defaultGraphNodeGenerated;
        case S:
            return t.getMatchSubject();
        case P:
            return t.getMatchPredicate();
        case O:
            return t.getMatchObject();
        default:
            return null;
        }
    }

    /**
     * returns the proper node from the triple
     * 
     * @param t the triple to get node from.
     * @return the node or null (not Node.ANY)
     */
    public Node from(Quad q) {
        switch (this) {
        case G:
            return q.getGraph();
        default:
            return from(q.asTriple());
        }
    }
}
