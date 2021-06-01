package org.streamreasoning.rsp4j.abstraction;

import lombok.extern.log4j.Log4j;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOp;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.querying.ContinuousQueryExecution;
import org.streamreasoning.rsp4j.api.querying.result.SolutionMapping;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.api.stream.data.WebDataStream;
import org.streamreasoning.rsp4j.yasper.ContinuousQueryExecutionImpl;
import org.streamreasoning.rsp4j.yasper.ContinuousQueryExecutionObserver;
import org.streamreasoning.rsp4j.yasper.examples.RDFStream;
import org.streamreasoning.rsp4j.yasper.querying.operators.R2RImpl;
import org.streamreasoning.rsp4j.yasper.querying.operators.Rstream;

import java.util.*;
import java.util.stream.Stream;

@Log4j
public class ContinuousProgram<I,R,O> extends ContinuousQueryExecutionObserver<I,R,O> {

    private List<Task<I,R,O>> tasks;
    private WebDataStream<I> inputStream;
    private WebDataStream<O> outputStream;
    private SDS<R> sds;



    public ContinuousProgram(ContinuousProgramBuilder builder){
        super(builder.sds,null);
        this.tasks = builder.tasks;
        this.inputStream = builder.inputStream;
        this.outputStream = builder.outputStream;
        this.sds = builder.sds;

        linkStreamsToOperators();
    }

    private void linkStreamsToOperators(){
        for(Task task: tasks){
            Set<Task.S2RContainer> s2rs = task.getS2Rs();
            for(Task.S2RContainer s2rContainer :s2rs){
                String streamURI = s2rContainer.getSourceURI();
                String tvgName = s2rContainer.getTvgName();
                IRI iri = RDFUtils.createIRI(streamURI);

                if(inputStream!=null) {
                    StreamToRelationOp<I, R> s2r = s2rContainer.getS2rFactory().apply(inputStream, iri);
                    s2r.link(this);
                    TimeVarying<R> tvg = s2r.get();

                    if (tvg.named()) {
                        sds.add(iri, tvg);
                    } else {
                        sds.add(tvg);
                    }
                }else{
                    log.error(String.format("No stream found for IRI %s",streamURI));
                }
            }
        }
    }

    @Override
    public void update(Observable o, Object arg) {
        Long now = (Long) arg;
        for (Task task : tasks) {
          Set<Task.R2SContainer> r2ss = task.getR2Ss();
          for (Task.R2SContainer r2s : r2ss) {
              eval(now).forEach(o1 -> outstream().put((O) r2s.getR2rFactory().eval(o1, now), now));
          }
        }

    }

    @Override
    public WebDataStream<O> outstream() {
        return outputStream;
    }

    @Override
    public ContinuousQuery query() {
        return null;
    }

    @Override
    public SDS<R> sds() {
        return sds;
    }

    @Override
    public StreamToRelationOp<I, R>[] s2rs() {
        return new StreamToRelationOp[0];
    }

    @Override
    public RelationToRelationOperator<O> r2r() {
        return null;
    }

    @Override
    public RelationToStreamOperator<O> r2s() {
        return null;
    }

    @Override
    public void add(StreamToRelationOp<I, R> op) {
        op.link(this);
    }

    public Stream<SolutionMapping<O>> eval(Long now) {
        sds.materialize(now);


        return tasks.get(0).getR2Rs().get(0).getR2rFactory().eval(now);
    }



    public static class ContinuousProgramBuilder<I,R,O>{
        private List<Task<I,R,O>> tasks;
        private WebDataStream<I> inputStream;
        private WebDataStream<O> outputStream;
        private SDS<I> sds;

        public ContinuousProgramBuilder(){
            tasks = new ArrayList<>();
        }

        public ContinuousProgramBuilder in(WebDataStream<I> stream){
            this.inputStream = stream;
            return this;
        }
        public ContinuousProgramBuilder out(WebDataStream<O> stream){
            this.outputStream = stream;
            return this;
        }

        public ContinuousProgramBuilder addTask(Task task){
            tasks.add(task);
            return this;
        }
        public ContinuousProgramBuilder setSDS(SDS<I> sds){
            this.sds = sds;
            return this;
        }

        public ContinuousProgram build(){
            return new ContinuousProgram(this);
        }

    }
}