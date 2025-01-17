package org.streamreasoning.rsp4j.yasper;

import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.querying.ContinuousQueryExecution;
import org.streamreasoning.rsp4j.api.sds.SDS;
import lombok.AllArgsConstructor;

import java.util.Observable;
import java.util.Observer;

/**
 * Created by riccardo on 03/07/2017.
 */
@AllArgsConstructor
public abstract class ContinuousQueryExecutionObserver<I, E1, E2> extends Observable implements Observer, ContinuousQueryExecution<I, E1, E2> {

    protected ContinuousQuery query;
    protected RelationToStreamOperator s2r;
    protected SDS sds;

    public ContinuousQueryExecutionObserver(SDS sds, ContinuousQuery query) {
        this.query = query;
        this.sds = sds;
    }

}
