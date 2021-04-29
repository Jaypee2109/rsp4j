package org.streamreasoning.rsp4j.yasper.engines;

import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.engine.config.EngineConfiguration;
import org.streamreasoning.rsp4j.api.engine.features.QueryRegistrationFeature;
import org.streamreasoning.rsp4j.api.engine.features.StreamRegistrationFeature;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.format.QueryResultFormatter;
import org.streamreasoning.rsp4j.api.operators.s2r.StreamToRelationOperatorFactory;
import org.streamreasoning.rsp4j.api.operators.s2r.syntax.WindowNode;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.querying.ContinuousQueryExecution;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.stream.data.WebDataStream;
import org.streamreasoning.rsp4j.api.stream.web.WebStream;
import org.streamreasoning.rsp4j.yasper.ContinuousQueryExecutionImpl;
import org.streamreasoning.rsp4j.yasper.examples.RDFStream;
import org.streamreasoning.rsp4j.yasper.examples.RDFTripleStream;
import org.streamreasoning.rsp4j.yasper.querying.operators.R2RImpl;
import org.streamreasoning.rsp4j.yasper.querying.operators.Rstream;
import org.streamreasoning.rsp4j.yasper.sds.SDSImpl;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Yasper implements QueryRegistrationFeature<ContinuousQuery>, StreamRegistrationFeature<RDFStream, RDFStream> {

    private final long t0;
    private final String baseUri;
    private final String windowOperatorFactory;
    private final String S2RFactory = "yasper.window_operator_factory";
    private Report report;
    private Tick tick;
    protected EngineConfiguration rsp_config;
    protected Map<String, SDS> assignedSDS;
    protected Map<String, ContinuousQueryExecution> queryExecutions;
    protected Map<String, ContinuousQuery> registeredQueries;
    protected Map<String, List<QueryResultFormatter>> queryObservers;
    protected Map<String, WebDataStream<Graph>> registeredStreams;
    private ReportGrain report_grain;


    public Yasper(EngineConfiguration rsp_config) {
        this.rsp_config = rsp_config;
        this.report = rsp_config.getReport();
        this.baseUri = rsp_config.getBaseIRI();
        this.report_grain = rsp_config.getReportGrain();
        this.tick = rsp_config.getTick();
        this.t0 = rsp_config.gett0();
        this.windowOperatorFactory = rsp_config.getString(S2RFactory);
        this.assignedSDS = new HashMap<>();
        this.registeredQueries = new HashMap<>();
        this.registeredStreams = new HashMap<>();
        this.queryObservers = new HashMap<>();
        this.queryExecutions = new HashMap<>();

    }

    @Override
    public ContinuousQueryExecution<Graph, Graph, Triple> register(ContinuousQuery q) {
//        return new ContinuousQueryExecutionFactoryImpl(q, windowOperatorFactory, registeredStreams, report, report_grain, tick, t0).build();

        SDS sds = new SDSImpl();

        RDFTripleStream out = new RDFTripleStream(q.getID());

        ContinuousQueryExecution<Graph, Graph, Triple> cqe = new ContinuousQueryExecutionImpl<Graph, Graph, Triple>(sds, q, null, out, new R2RImpl(sds, q), new Rstream());

        q.getWindowMap().forEach((WindowNode wo, WebStream s) -> {
            try {
                StreamToRelationOperatorFactory<Graph, Graph> w;
                IRI iri = RDFUtils.createIRI(wo.iri());

                Class<?> aClass = Class.forName(windowOperatorFactory);
                w = (StreamToRelationOperatorFactory<Graph, Graph>) aClass
                        .getConstructor(long.class,
                                long.class,
                                long.class,
                                Time.class,
                                Tick.class,
                                Report.class,
                                ReportGrain.class,
                                ContinuousQueryExecution.class)
                        .newInstance(wo.getRange(),
                                wo.getStep(),
                                wo.getT0(),
                                q.getTime(),
                                tick,
                                report,
                                report_grain,
                                cqe);

//            if (wo.getStep() == -1) {
//                w = new
//                (wo.getRange(), wo.getT0(), query.getTime(), tick, report, reportGrain, cqe);
//            } else
//                w = new CSPARQLTimeWindowOperatorFactory(wo.getRange(), wo.getStep(), wo.getT0(), query.getTime(), tick, report, reportGrain, cqe);

                TimeVarying<Graph> tvg = w.apply(registeredStreams.get(s.uri()), iri);

                if (wo.named()) {
                    sds.add(iri, tvg);
                } else {
                    sds.add(tvg);
                }

            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        });
        return cqe;
    }

    @Override
    public RDFStream register(RDFStream s) {
        registeredStreams.put(s.uri(), s);
        return s;
    }
}
