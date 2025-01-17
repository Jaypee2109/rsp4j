package org.streamreasoning.rsp4j.api.format;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.querying.ContinuousQueryExecution;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Created by riccardo on 03/07/2017.
 */

@Getter
@RequiredArgsConstructor
public abstract class QueryResultFormatter<O> implements Consumer<O> {

    protected final String format;
    protected final boolean distinct;
    protected ContinuousQueryExecution cqe;

}
