package it.polimi.jasper.engine.stream.items;

import it.polimi.yasper.core.stream.StreamSchema;

public abstract class RDFStreamSchema implements StreamSchema {

    protected final Class<?> type;

    public RDFStreamSchema(Class<?> clazz) {
        this.type = clazz;
    }

    @Override
    public Class getType() {
        return type;
    }

}