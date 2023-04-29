package org.xenei.rdfstore.txn;

import static java.lang.ThreadLocal.withInitial;
import static org.apache.jena.query.ReadWrite.WRITE;
import static org.apache.jena.query.ReadWrite.READ;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.shared.Lock;
import org.apache.jena.shared.LockMRPlusSW;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.mem.TransactionalComponent;

public class TxnHandler implements TransactionalComponent, TxnId {

    /**
     * This lock imposes the multiple-reader and single-writer policy of
     * transactions
     */
    private final Lock transactionLock = new LockMRPlusSW();

    private final ThreadLocal<Boolean> isInTransaction = withInitial(() -> false);
    private final ThreadLocal<ReadWrite> transactionMode = withInitial(() -> null);

    private Consumer<ReadWrite> prepareBegin;
    private TxnExec execCommit;
    private TxnExec execAbort;
    private TxnExec execEnd;
    private TxnId txnId;

    public TxnHandler(TxnId txnId, Consumer<ReadWrite> prepareBegin, TxnExec execCommit, TxnExec execAbort, TxnExec execEnd)
    {
        this.txnId = txnId;
        this.prepareBegin = prepareBegin;
        this.execCommit = execCommit;
        this.execAbort = execAbort;
        this.execEnd = execEnd;
    }
    
    public void setTxnId(TxnId prefix) {
        txnId = TxnId.setParent(prefix, txnId);
    }
    
    @Override
    public String txnId() {
        return txnId.txnId();
    }

    public boolean isInTransaction() {
        return isInTransaction.get();
    }

    public ReadWrite transactionMode() {
        return transactionMode.get();
    }
    
    private void logState(String action, ReadWrite readWrite) {
        System.out.println( String.format( "%s %s %s Txn:%s mode:%s",action, txnId.txnId(), readWrite, isInTransaction.get(), transactionMode.get()));
    }
    
    public void log(String str) {
        System.out.println( String.format( "%s %s ", txnId.txnId(),  str ));
    }

    @Override
    public void begin(ReadWrite readWrite) {
        logState("begin", readWrite);
        if (isInTransaction.get())
            throw new JenaTransactionException("Transactions cannot be nested!");
        isInTransaction.set(true);
        transactionLock.enterCriticalSection(readWrite.equals(READ)); // get the dataset write lock, if
                                                                                // needed.
        transactionMode.set(readWrite);
        prepareBegin.accept(readWrite);
    }

    @Override
    public void commit() {
        logState("commit",null);
        if (!isInTransaction.get())
            throw new JenaTransactionException("Tried to commit outside a transaction!");
        if (transactionMode().equals(WRITE)) {
            execCommit.exec();
        }
        finishTransaction();
    }

    /** Called transaction ending code at most once per transaction. */
    private void finishTransaction() {
        isInTransaction.remove();
        transactionMode.remove();
        transactionLock.leaveCriticalSection();
    }

    @Override
    public void abort() {
        logState("abort",null);
        if (!isInTransaction())
            throw new JenaTransactionException("Tried to abort outside a transaction!");
        if (transactionMode().equals(WRITE))
            execAbort.exec();
        finishTransaction();
    }

    @Override
    public void end() {
        logState("end",null);
        if (isInTransaction()) {
            if (transactionMode().equals(WRITE)) {
                String msg = "end() called for WRITE transaction without commit or abort having been called. This causes a forced abort.";
                // _abort does _end actions inside the lock.
                execAbort.exec();
                finishTransaction();
                throw new JenaTransactionException(msg);
            } 
            execEnd.exec();
            finishTransaction();
        }
    }
    
    private boolean startTxnIfNeeded(ReadWrite readWrite) {
        if (!isInTransaction()) {
            begin(readWrite);
            return true;
        } else if (readWrite.equals(WRITE) && !transactionMode().equals(WRITE)) {
            throw new JenaTransactionException("WRITE mode transaction required");
        }
        return false;
    }
    
    
    public <T> T doInTxn( ReadWrite readWrite, Supplier<T> supplier ) {
        boolean started = startTxnIfNeeded(readWrite);
        try {
            T result = supplier.get();
            if (started) {
                if (readWrite.equals(WRITE)) {
                    commit();
                } else {
                    end();
                }
            }
            return result;
        } catch (Exception e) {
            if (started) {
                abort();
            }
            throw e;
        }
    }
    
    public void doInTxn( ReadWrite readWrite, TxnExec exec ) {
        doInTxn( readWrite, () -> { exec.exec(); return null;});
    }
}
