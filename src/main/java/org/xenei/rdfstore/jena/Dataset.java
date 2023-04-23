package org.xenei.rdfstore.jena;

import java.util.Iterator;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.riot.system.PrefixMap;
import org.apache.jena.riot.system.PrefixMapStd;
import org.apache.jena.sparql.core.DatasetGraphTriplesQuads;
import org.apache.jena.sparql.core.Quad;
import org.xenei.rdfstore.store.Idx;
import org.xenei.rdfstore.store.Quads;

public class Dataset extends DatasetGraphTriplesQuads {
    private Quads quads;
    private PrefixMap prefixes;
    
    Dataset() {
        quads = new Quads();
        prefixes = new PrefixMapStd();
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        return quads.listNodes(Idx.G);
    }

    @Override
    public PrefixMap prefixes() {
        return prefixes;
    }

    @Override
    public boolean supportsTransactions() {
        return true;
    }

    @Override
    public void begin(TxnType type) {
        quads.begin(type);
    }

    @Override
    public boolean promote(Promote mode) {
        return quads.promote(mode);
    }

    @Override
    public void commit() {
        quads.commit();
    }

    @Override
    public void abort() {
        quads.abort();
    }

    @Override
    public void end() {
        quads.end();
    }

    @Override
    public ReadWrite transactionMode() {
        return quads.transactionMode();
    }

    @Override
    public TxnType transactionType() {
        return quads.transactionType();
    }

    @Override
    public boolean isInTransaction() {
        return quads.isInTransaction();
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
