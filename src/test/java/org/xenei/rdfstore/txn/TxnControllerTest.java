package org.xenei.rdfstore.txn;

import org.apache.jena.query.ReadWrite;
import org.apache.jena.sparql.JenaTransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.apache.jena.query.TxnType.READ;
import static org.apache.jena.query.TxnType.WRITE;


public class TxnControllerTest {
    
    TxnController controller;
    int beginCount = 0;
    int commitCount = 0;
    int abortCount = 0;
    int endCount = 0;
    int externActivity = 0;
    
    @BeforeEach
    public void setup() {
        controller = new TxnController(()->"Testing", this::execBegin, this::execCommit, this::execAbort, this::execEnd );
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
    
    private void assertCounts(int begin, int commit, int abort, int end) {
        assertEquals(  begin, beginCount, "Begin" );
        assertEquals( commit, commitCount, "Commit" );
        assertEquals( abort, abortCount, "Abort" );
        assertEquals( end, endCount, "End" );
    }
    
    @Test
    public void isInTransactionTest() {
        assertFalse( controller.isInTransaction() );
        controller.begin( READ );
        assertTrue( controller.isInTransaction() );
        controller.commit();
        assertFalse( controller.isInTransaction() );
        controller.begin(WRITE);
        assertTrue( controller.isInTransaction() );
        controller.commit();
        assertFalse( controller.isInTransaction() );
    }
    
    @Test
    public void startTransactonIfRequired() {
        controller.doInTxn(WRITE, () ->{ externActivity++;} );
        assertCounts( 1, 1, 0, 0 );
        assertEquals( 1, externActivity );
        
        assertEquals( 2, controller.doInTxn(WRITE, () -> { return ++externActivity;} ));
        assertCounts( 2, 2, 0, 0 );
        assertEquals( 2, externActivity );
        
        controller.begin( READ );
        assertCounts( 3, 2, 0, 0 );
        controller.doInTxn(READ, () ->{ externActivity++;} );
        assertCounts( 3, 2, 0, 0 );
        controller.end();
        assertCounts( 3, 2, 0, 1 );
    }
    
    @Test
    public void writeRequestWhileReadingFailsTest() {
        controller.begin( READ );
        assertCounts( 1, 0, 0, 0 );
        // transaction requesting WRITE in READ transaction fails.
        assertThrows(JenaTransactionException.class, 
                () -> controller.doInTxn(WRITE, () ->{ externActivity++;} ));
    }
    
    @Test
    public void readRequstWhileWritingPassesTest() {
        controller.begin( WRITE );
        assertCounts( 1, 0, 0, 0 );
        // transaction requesting READ in WRITE transaction works.
        controller.doInTxn(READ, () ->{ externActivity++;} );
        assertCounts( 1, 0, 0, 0 );
        controller.commit();
        assertCounts( 1, 1, 0, 0 );
    }
    
    @Test
    public void startTransactonIfRequiredRead() {
        controller.doInTxn(READ, () ->{ externActivity++;} );
        assertCounts( 1, 0, 0, 1 );
        assertEquals( 1, externActivity );
    }
    
    @Test
    public void commitTest() {
        controller.begin(READ);
        controller.commit();
        assertCounts( 1, 0, 0, 1 );

        controller.begin(WRITE);
        controller.commit();
        assertCounts( 2, 1, 0, 1 );
    }
    
    @Test
    public void abortTest() {
        controller.begin(READ);
        controller.abort();
        assertFalse( controller.isInTransaction() );
        assertCounts( 1, 0, 0, 1 );

        controller.begin(WRITE);
        controller.abort();
        assertFalse( controller.isInTransaction() );
        assertCounts( 2, 0, 1, 1 );
        
        assertThrows(JenaTransactionException.class, 
                () -> controller.abort() );
    }

    
    @Test
    public void endTest() {
        controller.begin(READ);
        controller.end();
        assertFalse( controller.isInTransaction() );
        assertCounts( 1, 0, 0, 1 );
        
        controller.begin(WRITE);
        assertThrows(JenaTransactionException.class, 
                () -> controller.end() );
        assertCounts( 2, 0, 1, 1 );
        
        controller.begin(WRITE);
        controller.commit();
        assertFalse( controller.isInTransaction() );
        assertCounts( 3, 1, 1, 1 );
        controller.end();
        assertCounts( 3, 1, 1, 1 );
    }
    
    @Test
    public void transactionModeTest() {
        assertNull( controller.transactionMode());
        controller.begin(READ);
        assertEquals( READ, controller.transactionMode());        
        controller.end();
        assertNull( controller.transactionMode());
        controller.begin(WRITE);
        assertEquals( WRITE, controller.transactionMode()); 
        controller.abort();
        assertNull( controller.transactionMode());
    }
}
