package org.xenei.rdfstore.txn;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.core.mem.TransactionalComponent;

public class TxnHandler extends TxnExecutor implements TransactionalComponent {

    private final Consumer<ReadWrite> prepareBegin;
    private final TxnExec commitF;
    private final TxnExec abortF;
    private final TxnExec endF;

    public TxnHandler(TxnId txnId, Consumer<ReadWrite> prepareBegin, TxnExec commitF, TxnExec abortF, TxnExec endF) {
        super(txnId);
        this.prepareBegin = prepareBegin;
        this.commitF = commitF;
        this.abortF = abortF;
        this.endF = endF;
    }

    @Override
    public void begin(ReadWrite readWrite) {
        begin(readWrite, prepareBegin);
    }

    @Override
    public void commit() {
        commit(commitF, endF);
    }

    @Override
    public void abort() {
        abort(abortF, endF);
    }

    @Override
    public void end() {
        end(abortF, endF);
    }

    public <T> T doInTxn(ReadWrite readWrite, Supplier<T> supplier) {
        return doInTxn(readWrite, supplier, prepareBegin, commitF, abortF, endF);
    }

    public void doInTxn(ReadWrite readWrite, TxnExec exec) {
        doInTxn(readWrite, exec, prepareBegin, commitF, abortF, endF);
    }
}
