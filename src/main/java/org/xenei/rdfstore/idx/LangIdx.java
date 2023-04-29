package org.xenei.rdfstore.idx;

/**
 * An index of language strings to URI values.
 *
 */
public class LangIdx extends AbstractIndex<String> {

    public LangIdx() {
        super(() -> "LangIdx");
    }
}