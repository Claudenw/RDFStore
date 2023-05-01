package org.xenei.rdfstore.txn;

import static org.apache.jena.query.ReadWrite.READ;
import static org.apache.jena.query.ReadWrite.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.JenaTransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TxnHandlerTest {

    TxnHandler handler;
    int beginCount = 0;
    int commitCount = 0;
    int abortCount = 0;
    int endCount = 0;
    int externActivity = 0;

    @BeforeEach
    public void setup() {
        handler = new TxnHandler(() -> "Testing", this::execBegin, this::execCommit, this::execAbort, this::execEnd);
        beginCount = 0;
        commitCount = 0;
        abortCount = 0;
        endCount = 0;
        externActivity = 0;
    }

    private void execBegin(ReadWrite readWrite) {
        beginCount++;
    }

    private void execCommit() {
        commitCount++;
    }

    private void execAbort() {
        abortCount++;
    }

    private void execEnd() {
        endCount++;
    }

    private void assertCounts(int begin, int commit, int abort, int end) {
        assertEquals(begin, beginCount, "Begin");
        assertEquals(commit, commitCount, "Commit");
        assertEquals(abort, abortCount, "Abort");
        assertEquals(end, endCount, "End");
    }

    @Test
    public void isInTransactionTest() {
        assertFalse(handler.isInTransaction());
        handler.begin(READ);
        assertTrue(handler.isInTransaction());
        handler.commit();
        assertFalse(handler.isInTransaction());
        handler.begin(WRITE);
        assertTrue(handler.isInTransaction());
        handler.commit();
        assertFalse(handler.isInTransaction());
    }

    @Test
    public void startTransactonIfRequired() {
        handler.doInTxn(WRITE, () -> {
            externActivity++;
        });
        assertCounts(1, 1, 0, 0);
        assertEquals(1, externActivity);

        assertEquals(2, handler.doInTxn(WRITE, () -> {
            return ++externActivity;
        }));
        assertCounts(2, 2, 0, 0);
        assertEquals(2, externActivity);

        handler.begin(READ);
        assertCounts(3, 2, 0, 0);
        handler.doInTxn(READ, () -> {
            externActivity++;
        });
        assertCounts(3, 2, 0, 0);
        handler.end();
        assertCounts(3, 2, 0, 1);
    }

    @Test
    public void writeRequestWhileReadingFailsTest() {
        handler.begin(READ);
        assertCounts(1, 0, 0, 0);
        // transaction requesting WRITE in READ transaction fails.
        assertThrows(JenaTransactionException.class, () -> handler.doInTxn(WRITE, () -> {
            externActivity++;
        }));
    }

    @Test
    public void readRequstWhileWritingPassesTest() {
        handler.begin(WRITE);
        assertCounts(1, 0, 0, 0);
        // transaction requesting READ in WRITE transaction works.
        handler.doInTxn(READ, () -> {
            externActivity++;
        });
        assertCounts(1, 0, 0, 0);
        handler.commit();
        assertCounts(1, 1, 0, 0);
    }

    @Test
    public void startTransactonIfRequiredRead() {
        handler.doInTxn(READ, () -> {
            externActivity++;
        });
        assertCounts(1, 0, 0, 1);
        assertEquals(1, externActivity);
    }

    @Test
    public void commitTest() {
        handler.begin(READ);
        handler.commit();
        assertCounts(1, 0, 0, 1);

        handler.begin(WRITE);
        handler.commit();
        assertCounts(2, 1, 0, 1);
    }

    @Test
    public void abortTest() {
        handler.begin(READ);
        handler.abort();
        assertFalse(handler.isInTransaction());
        assertCounts(1, 0, 0, 1);

        handler.begin(WRITE);
        handler.abort();
        assertFalse(handler.isInTransaction());
        assertCounts(2, 0, 1, 1);

        assertThrows(JenaTransactionException.class, () -> handler.abort());
    }

    @Test
    public void endTest() {
        handler.begin(READ);
        handler.end();
        assertFalse(handler.isInTransaction());
        assertCounts(1, 0, 0, 1);

        handler.begin(WRITE);
        assertThrows(JenaTransactionException.class, () -> handler.end());
        assertCounts(2, 0, 1, 1);

        handler.begin(WRITE);
        handler.commit();
        assertFalse(handler.isInTransaction());
        assertCounts(3, 1, 1, 1);
        handler.end();
        assertCounts(3, 1, 1, 1);
    }

    @Test
    public void transactionModeTest() {
        assertNull(handler.transactionMode());
        handler.begin(READ);
        assertEquals(READ, handler.transactionMode());
        handler.end();
        assertNull(handler.transactionMode());
        handler.begin(WRITE);
        assertEquals(WRITE, handler.transactionMode());
        handler.abort();
        assertNull(handler.transactionMode());
    }
}
