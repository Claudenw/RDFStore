package org.xenei.rdfstore.store;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.impl.LiteralLabel;
import org.xenei.rdfstore.Store;
import org.xenei.rdfstore.TrieStore;
import org.xenei.rdfstore.idx.Bitmap;
import org.xenei.rdfstore.idx.LangIdx;
import org.xenei.rdfstore.idx.NumberIdx;

public class URIs {
    private TrieStore<Node> store = new TrieStore<Node>( URIs::asString );
    private NumberIdx numbers = new NumberIdx();
    private LangIdx languages = new LangIdx();

    public static class Result {

        private final Bitmap[] bitmap = new Bitmap[3];

        public void set(Idx idx, int triple) {
            int i = idx.ordinal();
            if (bitmap[i] == null) {
                bitmap[i] = new Bitmap();
            }
            bitmap[i].set(triple);
        }

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

    private static String asString(Node node) {
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
    
    private int asInt(long l) {
        return (int) l;
    }

    public long register(Node node) {
        // String key = asString(node);
        Store.Result result = store.register(node);
        if (!result.existed) {
            if (node.isLiteral()) {

                LiteralLabel label = node.getLiteral();
                if (label.isXML()) {
                    if (Number.class.isAssignableFrom(label.getDatatype().getJavaClass())) {
                        numbers.register((Number) label.getValue(), asInt(result.index));
                    }
                } else {
                    languages.register(node.getLiteral().language(), asInt(result.index));
                }
            }
        }
        return result.index;
    }

    public Node get(long idx) {
        return store.get(idx);
    }

}
