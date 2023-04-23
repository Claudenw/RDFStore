package org.xenei.rdfstore.store;

import static java.lang.ThreadLocal.withInitial;
import static org.apache.jena.query.ReadWrite.WRITE;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.shared.Lock;
import org.apache.jena.shared.LockMRPlusSW;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;
import org.apache.jena.sparql.core.Transactional.Promote;
import org.apache.jena.sparql.core.mem.TransactionalComponent;
import org.xenei.rdfstore.Store;
import org.xenei.rdfstore.TrieStore;
import org.xenei.rdfstore.idx.Bitmap;
import org.xenei.rdfstore.idx.LangIdx;
import org.xenei.rdfstore.idx.NumberIdx;
import org.slf4j.Logger;

public class URIs implements TransactionalComponent {
    
    private static final Logger log = getLogger(URIs.class);

    private TrieStore<Node> store = new TrieStore<Node>(URIs::asString);
    private NumberIdx numbers = new NumberIdx();
    private LangIdx languages = new LangIdx();


 
    
    //** ACCESS CODE 
    
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
