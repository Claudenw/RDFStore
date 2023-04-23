package org.xenei.rdfstore.store;

import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;

import java.util.Iterator;
import java.util.PrimitiveIterator;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.mem.TransactionalComponent;
import org.xenei.rdfstore.Store;
import org.xenei.rdfstore.TrieStore;
import org.xenei.rdfstore.idx.AbstractIndex;
import org.xenei.rdfstore.idx.Bitmap;
import org.xenei.rdfstore.idx.LangIdx;
import org.xenei.rdfstore.idx.NumberIdx;
import org.xenei.rdfstore.txn.TxnHandler;

public class URIs implements TransactionalComponent {
    private final TrieStore<Node> store;
    private final AbstractIndex<Number> numbers;
    private final AbstractIndex<String> languages;
    private final TxnHandler txnHandler;

    public URIs() {
        store = new TrieStore<Node>(URIs::asString);
        numbers = new NumberIdx();
        languages = new LangIdx();
        txnHandler = new TxnHandler(this::prepareBegin, this::execCommit, this::execAbort, this::execEnd);
    }

    // ** ACCESS CODE

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

    private void prepareBegin(ReadWrite readWrite) {
        store.begin(readWrite);
        languages.begin(readWrite);
        numbers.begin(readWrite);
    }

    private void execCommit() {
        store.commit();
        languages.commit();
        numbers.commit();
    }

    private void execAbort() {
        store.abort();
        languages.abort();
        numbers.abort();
    }

    private void execEnd() {
        store.end();
        languages.end();
        numbers.end();
    }

    public long register(Node node) {
        return txnHandler.doInTxn(WRITE, () -> {
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
        });
    }

    public Node get(long idx) {
        return txnHandler.doInTxn(READ, () -> {
            return store.get(idx);
        });
    }

    public Iterator<Node> iterator(PrimitiveIterator.OfLong iter) {
        return new Iterator<Node>() {
            PrimitiveIterator.OfLong i = iter;

            @Override
            public boolean hasNext() {
                return i.hasNext();
            }

            @Override
            public Node next() {
                return get(i.next());
            }

        };
    }

    @Override
    public void begin(ReadWrite readWrite) {
        txnHandler.begin(readWrite);
    }

    @Override
    public void commit() {
        txnHandler.commit();
    }

    @Override
    public void abort() {
        txnHandler.abort();
    }

    @Override
    public void end() {
        txnHandler.end();
    }

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

}
