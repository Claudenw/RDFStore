package org.xenei.rdfstore.txn;

import static java.lang.ThreadLocal.withInitial;
import static org.apache.jena.query.ReadWrite.WRITE;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.shared.Lock;
import org.apache.jena.sparql.JenaTransactionException;
import org.apache.jena.sparql.core.Transactional;

public class TxnController extends TxnExecutor implements Transactional {

    /**
     * Dataset version. A write transaction increments this in commit.
     */
    private final AtomicLong generation = new AtomicLong(0);
    private final ThreadLocal<Long> version = withInitial(() -> 0L);

    private final ThreadLocal<TxnType> transactionType = withInitial(() -> null);

    /**
     * Transaction lifecycle operations must be atomic, especially
     * {@link Transactional#begin} and {@link Transactional#commit}.
     * <p>
     * There are changes to be made to several datastructures and this insures that
     * they are made consistently.
     * <p>
     * Lock order must be writer lock, system lock. If a transaction takes the
     * writerLock, it is a writer, and there is only one writer.
     */
    private final ReentrantLock systemLock = new ReentrantLock(true);

    private final Consumer<ReadWrite> prepareBegin;
    private final TxnExec commitF;
    private final TxnExec abortF;
    private final TxnExec endF;

    public TxnController(TxnId txnId, Consumer<ReadWrite> prepareBegin, TxnExec commitF, TxnExec abortF, TxnExec endF) {
        super(txnId);
        this.prepareBegin = prepareBegin;
        this.commitF = execInLock(commitF.andThen(() -> {
            if (transactionMode().equals(WRITE)) {
                if (version.get() != generation.get()) {
                    throw new InternalErrorException(
                            String.format("Version=%d, Generation=%d", version.get(), generation.get()));
                }
                generation.incrementAndGet();
            }
        }));
        this.abortF = execInLock(abortF);
        this.endF = execInLock(endF);
        this.finishTransaction = finishTransaction.andThen(() -> {
            transactionType.remove();
            version.remove();
        });
    }

    @Override
    protected void logState(String action, ReadWrite readWrite) {
        System.out.println(String.format("%s %s %s Txn:%s mode:%s ver:%s gen:%s", action, txnId(), readWrite,
                isInTransaction(), transactionMode(), version.get(), generation.get()));
    }

    private static void withLock(java.util.concurrent.locks.Lock lock, Runnable action) {
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void begin(TxnType type) {
        super.begin(TxnType.initial(type), new BeginConsumer(type));
    }

    @Override
    public boolean promote(Promote mode) {
        if (!isInTransaction())
            throw new JenaTransactionException("Tried to promote outside a transaction!");
        if (transactionMode().equals(ReadWrite.WRITE))
            return true;

        if (transactionType() == TxnType.READ)
            return false;

        boolean readCommitted = (mode == Promote.READ_COMMITTED);

        try {
            _promote(readCommitted);
            return true;
        } catch (JenaTransactionException ex) {
            return false;
        }
    }

    private void _promote(boolean readCommited) {
        // Outside lock.
        if (!readCommited && version.get() != generation.get()) {
            // This tests for any committed writers since this transaction started.
            // This does not catch the case of a currently active writer
            // that has not gone to commit or abort yet.
            // The final test is after we obtain the transactionLock.
            throw new JenaTransactionException("Dataset changed - can't promote");
        }

        // Blocking on other writers.
        enterCriticalSection(Lock.WRITE);
        // Check again now we are inside the lock.
        if (!readCommited && version.get() != generation.get()) {
            // Can't promote - release the lock.
            leaveCriticalSection();
            throw new JenaTransactionException("Concurrent writer changed the dataset : can't promote");
        }
        // We have the writer lock and we have promoted!

        isInTransaction(true);
        transactionMode(ReadWrite.WRITE);
        prepareBegin.accept(ReadWrite.WRITE);
        version.set(generation.get());
    }

    private TxnExec execInLock(TxnExec func) {
        return () -> withLock(systemLock, func);
    }

    @Override
    public void commit() {
        super.commit(commitF, endF);
    }

    @Override
    public void abort() {
        super.abort(abortF, endF);
    }

    @Override
    public void end() {
        super.end(abortF, endF);
    }

    public <T> T doInTxn(TxnType txnType, Supplier<T> supplier) {

        return doInTxn(TxnType.initial(txnType), supplier, new BeginConsumer(txnType), commitF, abortF, endF);
    }

    public void doInTxn(TxnType txnType, TxnExec exec) {

        doInTxn(TxnType.initial(txnType), exec, new BeginConsumer(txnType), commitF, abortF, endF);
    }

    @Override
    public TxnType transactionType() {
        return transactionType.get();
    }

    private class BeginConsumer implements Consumer<ReadWrite> {
        Consumer<ReadWrite> inner;

        BeginConsumer(TxnType type) {
            inner = (readWrite) -> transactionType.set(type);
            inner = inner.andThen(prepareBegin).andThen((rw) -> version.set(generation.get()));
        }

        @Override
        public void accept(ReadWrite arg0) {
            systemLock.lock();
            try {
                inner.accept(arg0);
                logState("after begin", arg0);
            } finally {
                systemLock.unlock();
            }
        }
    };
}
