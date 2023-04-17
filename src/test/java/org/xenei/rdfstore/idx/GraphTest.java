package org.xenei.rdfstore.idx;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DC;
import org.junit.jupiter.api.Test;
import org.xenei.rdfstore.jena.Graph;

public class GraphTest {
    @Test
    public void x() {
        Graph g = new org.xenei.rdfstore.jena.Graph();
        Model m = ModelFactory.createModelForGraph( g );
        Resource r = m.createResource();
        m.add( r, DC.title, "Test" );
        m.listStatements( null, DC.title, "test");
        StmtIterator iter = m.listStatements();
        while (iter.hasNext()) {
            System.out.println( iter.next().toString() );
        }
        m.write( System.out, Lang.TURTLE.getName() );
        System.out.println( "done");
    }
}
