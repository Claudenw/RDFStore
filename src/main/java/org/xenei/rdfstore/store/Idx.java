package org.xenei.rdfstore.store;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public enum Idx {
    S, P, O;

    /**
     * returns the proper node from the triple
     * 
     * @param t the triple to get node from.
     * @return the node or null (not Node.ANY)
     */
    public Node from(Triple t) {
        switch (this) {
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

}
