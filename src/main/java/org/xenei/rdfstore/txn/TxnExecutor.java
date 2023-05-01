package org.xenei.rdfstore.txn;

import static java.lang.ThreadLocal.withInitial;
import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.shared.Lock;
import org.apache.jena.shared.LockMRPlusSW;
import org.apache.jena.sparql.JenaTransactionException;

public class TxnExecutor implements TxnId {

    /**
     * This lock imposes the multiple-reader and single-writer policy of
     * transactions
     */
    private final Lock transactionLock = new LockMRPlusSW();

    private final ThreadLocal<Boolean> isInTransaction = withInitial(() -> false);
    private final ThreadLocal<ReadWrite> transactionMode = withInitial(() -> null);

    private TxnId txnId;

    protected TxnExec finishTransaction = () -> {
        isInTransaction.remove();
        transactionMode.remove();
        transactionLock.leaveCriticalSection();
    };

    public TxnExecutor(TxnId txnId) {
        this.txnId = txnId;
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

    protected void isInTransaction(boolean state) {
        isInTransaction.set(state);
    }

    public ReadWrite transactionMode() {
        return transactionMode.get();
    }

    protected void transactionMode(ReadWrite mode) {
        transactionMode.set(mode);
    }

    protected void logState(String action, ReadWrite readWrite) {
        System.out.println(String.format("%s %s %s Txn:%s mode:%s", action, txnId.txnId(), readWrite,
                isInTransaction.get(), transactionMode.get()));
    }

    public void log(String str) {
        System.out.println(String.format("%s %s ", txnId.txnId(), str));
    }

    public void begin(ReadWrite readWrite, Consumer<ReadWrite> func) {
        logState("begin", readWrite);
        if (isInTransaction.get())
            throw new JenaTransactionException("Transactions cannot be nested!");
        isInTransaction.set(true);
        transactionLock.enterCriticalSection(readWrite.equals(READ)); // get the dataset write lock, if
                                                                      // needed.
        transactionMode.set(readWrite);
        func.accept(readWrite);
    }

    public void commit(TxnExec commitF, TxnExec endF) {
        logState("commit", null);
        if (!isInTransaction.get())
            throw new JenaTransactionException("Tried to commit outside a transaction!");
        if (transactionMode().equals(WRITE)) {
            commitF.run();
        } else {
            endF.run();
        }
        finishTransaction.run();
    }

    public void abort(TxnExec abortFunc, TxnExec endFunc) {
        logState("abort", null);
        if (!isInTransaction())
            throw new JenaTransactionException("Tried to abort outside a transaction!");
        if (transactionMode().equals(WRITE)) {
            abortFunc.run();
        } else {
            endFunc.run();
        }
        finishTransaction.run();
    }

    public void end(TxnExec abortFunc, TxnExec endFunc) {
        logState("end", null);
        if (isInTransaction()) {
            if (transactionMode().equals(WRITE)) {
                String msg = "end() called for WRITE transaction without commit or abort having been called. This causes a forced abort.";
                // _abort does _end actions inside the lock.
                abortFunc.run();
                finishTransaction.run();
                throw new JenaTransactionException(msg);
            }
            endFunc.run();
            finishTransaction.run();
        }
    }

    private boolean startTxnIfNeeded(ReadWrite readWrite, Consumer<ReadWrite> beginF) {
        if (!isInTransaction()) {
            begin(readWrite, beginF);
            return true;
        } else if (readWrite.equals(WRITE) && !transactionMode().equals(WRITE)) {
            throw new JenaTransactionException("WRITE mode transaction required");
        }
        return false;
    }

    protected <T> T doInTxn(ReadWrite readWrite, Supplier<T> supplier, Consumer<ReadWrite> beginF, TxnExec commitF,
            TxnExec abortF, TxnExec endF) {
        boolean started = startTxnIfNeeded(readWrite, beginF);
        try {
            T result = supplier.get();
            if (started) {
                if (readWrite.equals(WRITE)) {
                    commit(commitF, endF);
                } else {
                    end(abortF, endF);
                }
            }
            return result;
        } catch (Exception e) {
            if (started) {
                abort(abortF, endF);
            }
            throw e;
        }
    }

    protected void doInTxn(ReadWrite readWrite, TxnExec exec, Consumer<ReadWrite> beginF, TxnExec commitF,
            TxnExec abortF, TxnExec endF) {
        doInTxn(readWrite, () -> {
            exec.run();
            return null;
        }, beginF, commitF, abortF, endF);
    }

    public void enterCriticalSection(boolean readLockRequested) {
        transactionLock.enterCriticalSection(readLockRequested);
    }

    public void leaveCriticalSection() {
        transactionLock.leaveCriticalSection();
    }
}
