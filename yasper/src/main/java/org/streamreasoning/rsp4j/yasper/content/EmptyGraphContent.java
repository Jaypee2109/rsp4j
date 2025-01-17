package org.streamreasoning.rsp4j.yasper.content;

import org.apache.commons.rdf.api.Graph;
import org.streamreasoning.rsp4j.api.secret.content.Content;

public class EmptyGraphContent implements Content<Graph,Graph> {

    long ts = System.currentTimeMillis();

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void add(Graph e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getTimeStampLastUpdate() {
        return ts;
    }

    @Override
    public Graph coalesce() {
        return null;
    }
}
