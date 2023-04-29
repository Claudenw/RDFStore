package org.xenei.rdfstore.txn;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.JenaTransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.jena.query.ReadWrite.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.jena.query.ReadWrite.READ;


public class TxnHandlerTest {
    
    TxnHandler handler;
    int beginCount = 0;
    int commitCount = 0;
    int abortCount = 0;
    int endCount = 0;
    int externActivity = 0;
    
    @BeforeEach
    public void setup() {
        handler = new TxnHandler(()->"Testing", this::execBegin, this::execCommit, this::execAbort, this::execEnd );
        beginCount = 0;
        commitCount = 0;
        abortCount = 0;
        endCount = 0;
        externActivity = 0;
    }
    
    private void execBegin(ReadWrite readWrite) {beginCount++;}
    
    private void execCommit() {commitCount++;}
    
    private void execAbort() {abortCount++;}
    
    private void execEnd() {endCount++;}
    
    @Test
    public void isInTransactionTest() {
        assertFalse( handler.isInTransaction() );
        handler.begin( READ );
        assertTrue( handler.isInTransaction() );
        handler.commit();
        assertFalse( handler.isInTransaction() );
        handler.begin(WRITE);
        assertTrue( handler.isInTransaction() );
        handler.commit();
        assertFalse( handler.isInTransaction() );
    }
    
    @Test
    public void startTransactonIfRequire() {
        handler.doInTxn(WRITE, () ->{ externActivity++;} );
        assertEquals( 1, beginCount );
        assertEquals( 1, commitCount );
        assertEquals( 1, externActivity );
        assertEquals( 2, handler.doInTxn(WRITE, () -> { return ++externActivity;} ));
        assertEquals( 2, beginCount );
        assertEquals( 2, commitCount );
        handler.begin( READ );
        assertEquals( 3, beginCount );
        assertEquals( 2, externActivity );
        assertEquals( 2, commitCount );
        handler.doInTxn(READ, () ->{ externActivity++;} );
        assertEquals( 3, beginCount );
        assertEquals( 3, externActivity );
        assertEquals( 2, commitCount );
        // transaction requesting WRITE in READ transaction fails.
        assertThrows(JenaTransactionException.class, 
                () -> handler.doInTxn(WRITE, () ->{ externActivity++;} ));
    }
    
    @Test
    public void commitTest() {
        handler.begin(READ);
        handler.commit();
        assertEquals(1, beginCount);
        assertEquals(0, commitCount);
        handler.begin(WRITE);
        handler.commit();
        assertEquals(2, beginCount);
        assertEquals(1, commitCount);
    }
    
    @Test
    public void abortTest() {
        handler.begin(READ);
        handler.abort();
        assertFalse( handler.isInTransaction() );
        assertEquals(1, beginCount);
        assertEquals(0, commitCount);
        assertEquals(0, abortCount);
        handler.begin(WRITE);
        handler.abort();
        assertFalse( handler.isInTransaction() );
        assertEquals(2, beginCount);
        assertEquals(0, commitCount);
        assertEquals(1, abortCount);
        assertThrows(JenaTransactionException.class, 
                () -> handler.abort() );
    }

    
    @Test
    public void endTest() {
        handler.begin(READ);
        handler.end();
        assertFalse( handler.isInTransaction() );
        assertEquals(1, beginCount);
        assertEquals(0, commitCount);
        assertEquals(1, endCount);
        handler.begin(WRITE);
        assertThrows(JenaTransactionException.class, 
                () -> handler.end() );
        handler.begin(WRITE);
        handler.commit();
        assertFalse( handler.isInTransaction() );
        handler.end();
        assertEquals(3, beginCount);
        assertEquals(1, commitCount);
        assertEquals(1, endCount);
    }
    
    @Test
    public void transactionModeTest() {
        assertNull( handler.transactionMode());
        handler.begin(READ);
        assertEquals( READ, handler.transactionMode());        
        handler.end();
        assertNull( handler.transactionMode());
        handler.begin(WRITE);
        assertEquals( WRITE, handler.transactionMode()); 
        handler.abort();
        assertNull( handler.transactionMode());
    }
}
