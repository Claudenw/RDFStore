package org.xenei.rdfstore.store;

import java.util.Iterator;
import java.util.PrimitiveIterator;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.mem.TransactionalComponent;
import org.xenei.rdfstore.txn.TxnId;
import org.xenei.rdfstore.txn.TxnIdHolder;

public interface UriStore extends TransactionalComponent, TxnIdHolder {

    // ** ACCESS CODE

    static String asString(Node node) {
        if (node.isURI()) {
            return node.getURI();
        }
        if (node.isLiteral()) {
            return node.getLiteral().getLexicalForm();
        }
        if (node.isBlank()) {
            return node.getBlankNodeLabel();
        }
        return node.toString(true);
    }

    @Override
    void setTxnId(TxnId prefix);

    long register(Node node);

    long get(Node node);

    Node get(long idx);

    Iterator<Node> iterator(PrimitiveIterator.OfLong iter);

    static class Result {

        private final Bitmap[] bitmap = new Bitmap[3];

        public void clear(Idx idx, int triple) {
            int i = idx.ordinal();
            if (bitmap[i] != null) {
                bitmap[i].clear(triple);
            }
        }

        public Bitmap get(Idx idx) {
            return bitmap[idx.ordinal()];
        }
    }

}
