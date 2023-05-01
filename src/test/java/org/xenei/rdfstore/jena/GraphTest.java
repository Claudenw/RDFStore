package org.xenei.rdfstore.jena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DC_11;
import org.junit.jupiter.api.Test;

public class GraphTest {
    @Test
    public void automaticTransactionTest() {
        Graph g = new org.xenei.rdfstore.jena.Graph();
        Model m = ModelFactory.createModelForGraph(g);
        Resource r = m.createResource();
        m.add(r, DC_11.title, "Test");
        StmtIterator iter = m.listStatements(null, DC_11.title, "test");
        assertFalse(iter.hasNext());

        iter = m.listStatements(null, DC_11.title, "Test");
        assertTrue(iter.hasNext());
        Statement stmt = iter.next();
        assertEquals(r, stmt.getSubject());
        assertFalse(iter.hasNext());

        iter = m.listStatements();
        assertTrue(iter.hasNext());
        stmt = iter.next();
        assertEquals(r, stmt.getSubject());
        assertEquals(DC_11.title, stmt.getPredicate());
        assertEquals("Test", stmt.getObject().toString());
        assertFalse(iter.hasNext());

        m.write(System.out, Lang.TURTLE.getName());
        System.out.println("done");
    }

}
