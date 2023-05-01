package org.xenei.rdfstore.store;

import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Supplier;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.query.ReadWrite;
import org.xenei.rdfstore.idx.AbstractIndex.Mapper;
import org.xenei.rdfstore.idx.LangIdx;
import org.xenei.rdfstore.idx.NumberIdx;
import org.xenei.rdfstore.txn.TxnHandler;
import org.xenei.rdfstore.txn.TxnId;

public class AbstractUriStore implements UriStore {
    private final Store<Node> store;
    private final NumberIdx numbers;
    private final LangIdx languages;
    private final TxnHandler txnHandler;
    private final Supplier<Bitmap> bitmapSupplier;

    public AbstractUriStore(Store<Node> store, Mapper<BigDecimal> numbers, Mapper<String> languages,
            Supplier<Bitmap> bitmapSupplier) {
        TxnId txnId = () -> "URIs";
        this.store = store;
        this.numbers = new NumberIdx(bitmapSupplier, numbers);
        this.languages = new LangIdx(bitmapSupplier, languages);
        this.numbers.setTxnId(txnId);
        this.languages.setTxnId(txnId);
        this.store.setTxnId(txnId);
        this.bitmapSupplier = bitmapSupplier;
        txnHandler = new TxnHandler(txnId, this::prepareBegin, this::execCommit, this::execAbort, this::execEnd);
    }

    @Override
    public void setTxnId(TxnId prefix) {
        txnHandler.setTxnId(prefix);
        numbers.setTxnId(prefix);
        languages.setTxnId(prefix);
        store.setTxnId(prefix);
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

    @Override
    public long register(Node node) {
        return txnHandler.doInTxn(WRITE, () -> {
            // String key = asString(node);
            Store.Result result = store.register(node);
            if (!result.existed) {
                if (node.isLiteral()) {

                    LiteralLabel label = node.getLiteral();
                    if (label.isXML()) {
                        BigDecimal d = NumberIdx.parse(label);
                        if (d != null) {
                            numbers.register(d, result.index);
                        }
                    } else {
                        languages.register(node.getLiteral().language(), result.index);
                    }
                }
            }
            return result.index;
        });
    }

    @Override
    public long get(Node node) {
        return txnHandler.doInTxn(READ, () -> {
            return store.get(node);
        });
    }

    @Override
    public Node get(long idx) {
        return txnHandler.doInTxn(READ, () -> {
            return store.get(idx);
        });
    }

    @Override
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
}
