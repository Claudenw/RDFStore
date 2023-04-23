package org.xenei.rdfstore.jena;

import java.util.Iterator;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.sparql.core.DatasetGraphTriplesQuads;
import org.apache.jena.sparql.core.Quad;
import org.xenei.rdfstore.store.Idx;
import org.xenei.rdfstore.store.Quads;

public class Dataset extends DatasetGraphTriplesQuads {
    private Quads quads;

    @Override
    public Iterator<Node> listGraphNodes() {
        return quads.listNodes(Idx.G);
    }

    @Override
    public PrefixMap prefixes() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean supportsTransactions() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void begin(TxnType type) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean promote(Promote mode) {
        // TODO Auto-generated method stub
        return false;
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
    public ReadWrite transactionMode() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TxnType transactionType() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isInTransaction() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    protected void addToDftGraph(Node s, Node p, Node o) {
        quads.delete(Quad.create(Quad.defaultGraphNodeGenerated, s, p, o));
    }

    @Override
    protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
        quads.register(new Quad(g, s, p, o));
    }

    @Override
    protected void deleteFromDftGraph(Node s, Node p, Node o) {
        quads.delete(Quad.create(Quad.defaultGraphNodeGenerated, s, p, o));
    }

    @Override
    protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {
        quads.delete(new Quad(g, s, p, o));
    }

    @Override
    protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
        return quads.find(Quad.create(Quad.defaultGraphNodeGenerated, s, p, o), quads::asQuad);
    }

    @Override
    protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p, Node o) {
        return quads.find(Quad.create(g, s, p, o), quads::asQuad);
    }

    @Override
    protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
        return quads.find(Quad.create(null, s, p, o), quads::asQuad);
    }

    @Override
    public Graph getDefaultGraph() {
        return new org.xenei.rdfstore.jena.Graph(quads);
    }

    @Override
    public Graph getGraph(Node graphNode) {
        return new org.xenei.rdfstore.jena.Graph(quads, graphNode);
    }

}
