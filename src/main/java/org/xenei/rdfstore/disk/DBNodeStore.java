package org.xenei.rdfstore.disk;

import java.util.Iterator;

import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.xenei.rdfstore.store.IdxData;
import org.xenei.rdfstore.store.Store;
import org.xenei.rdfstore.txn.TxnId;

public class DBNodeStore implements Store<Node> {

    @Override
    public void begin(ReadWrite readWrite) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void commit() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void abort() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void end() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setTxnId(TxnId prefix) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Result register(Node item) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Result delete(Node item) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Node get(long idx) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long get(Node value) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean contains(Node item) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public long size() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Iterator<IdxData<Node>> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

}
