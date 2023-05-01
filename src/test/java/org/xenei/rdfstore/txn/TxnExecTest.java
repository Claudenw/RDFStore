package org.xenei.rdfstore.txn;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TxnExecTest {

    @Test
    public void andThenTest() {
        boolean values[] = new boolean[2];
        boolean two;
        TxnExec oneF = () -> values[0] = true;
        TxnExec twoF = () -> values[1] = true;

        TxnExec three = oneF.andThen(twoF);
        three.run();
        assertTrue(values[0]);
        assertTrue(values[1]);
    }
}
