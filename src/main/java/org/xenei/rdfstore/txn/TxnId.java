package org.xenei.rdfstore.txn;

@java.lang.FunctionalInterface
public interface TxnId {
    String txnId();

    public static TxnId setParent(TxnId parent, TxnId id) {
        return () -> String.format("%s->%s", parent.txnId(), id.txnId());
    }

}
