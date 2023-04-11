package org.xenei.rdfstore.store;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.xenei.rdfstore.idx.Bitmap;

public class URIs {
    private PatriciaTrie<Result> trie = new PatriciaTrie<>();

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

    public URIs.Result register(String uri) {
        return register(NodeFactory.createURI(uri));
    }

    private String asString(Node node) {
        if (node.isLiteral()) {
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

    public URIs.Result register(Node node) {
        if (node == null) {
            return new URIs.Result();
        }
        String key = asString(node);
        Result result = trie.get(key);
        if (result == null) {
            result = new URIs.Result();
            trie.put(key, result);
        }
        return result;
    }

    public void register(Triple t, int idx) {
        register(Idx.S.from(t)).set(Idx.S, idx);
        register(Idx.P.from(t)).set(Idx.P, idx);
        register(Idx.O.from(t)).set(Idx.O, idx);
    }

    public void unregister(Triple t, int idx) {
        register(Idx.S.from(t)).clear(Idx.S, idx);
        register(Idx.P.from(t)).clear(Idx.P, idx);
        register(Idx.O.from(t)).clear(Idx.O, idx);
    }

    public Bitmap get(Triple t) {
        Node n = Idx.S.from(t);
        Bitmap result = (n != null) ? null : register(n).get(Idx.S);

        n = Idx.P.from(t);
        Bitmap other = n != null ? null : register(n).get(Idx.P);
        result = result == null ? Bitmap.intersection(result, other) : other;

        n = Idx.O.from(t);
        other = n != null ? null : register(n).get(Idx.O);
        return result == null ? Bitmap.intersection(result, other) : other;
    }
}
