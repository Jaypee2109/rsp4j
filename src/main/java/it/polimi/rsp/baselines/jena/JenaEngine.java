package it.polimi.rsp.baselines.jena;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.ConfigurationMethodRef;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.time.CurrentTimeEvent;
import it.polimi.heaven.rsp.rsp.querying.ContinousQueryExecution;
import it.polimi.heaven.rsp.rsp.querying.Query;
import it.polimi.rsp.baselines.enums.OntoLanguage;
import it.polimi.rsp.baselines.enums.Reasoning;
import it.polimi.rsp.baselines.esper.RSPEsperEngine;
import it.polimi.rsp.baselines.esper.RSPListener;
import it.polimi.rsp.baselines.exceptions.StreamRegistrationException;
import it.polimi.rsp.baselines.jena.events.stimuli.BaselineStimulus;
import it.polimi.rsp.baselines.jena.query.BaselineQuery;
import it.polimi.rsp.baselines.jena.query.JenaCQueryExecution;
import it.polimi.streaming.EventProcessor;
import it.polimi.streaming.Response;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;

import java.util.HashMap;
import java.util.Map;

@Log4j
public abstract class JenaEngine extends RSPEsperEngine {

    protected final long t0;
    @Setter
    private Reasoning reasoning;
    @Setter
    private OntoLanguage ontology_language;

    private Map<Query, RSPListener> queries;
    protected final boolean internalTimerEnabled;

    public JenaEngine(BaselineStimulus eventType, EventProcessor<Response> receiver, long t0, String provider) {
        super(receiver, new Configuration());
        this.queries = new HashMap<Query, RSPListener>();
        this.internalTimerEnabled = false;
        this.t0 = t0;
        ref = new ConfigurationMethodRef();
        cepConfig.getEngineDefaults().getThreading().setInternalTimerEnabled(internalTimerEnabled);
        cepConfig.getEngineDefaults().getLogging().setEnableExecutionDebug(true);
        cepConfig.getEngineDefaults().getLogging().setEnableTimerDebug(true);

        log.info("Added [" + eventType + "] as TStream");
        cepConfig.addEventType("TEvent", eventType);
        cep = EPServiceProviderManager.getProvider(provider, cepConfig);
        cepAdm = cep.getEPAdministrator();
        cepRT = cep.getEPRuntime();


    }

    public JenaEngine(BaselineStimulus eventType, EventProcessor<Response> receiver, long t0, boolean internalTimerEnabled, String provider) {
        super(receiver, new Configuration());
        this.t0 = t0;
        this.queries = new HashMap<Query, RSPListener>();
        this.internalTimerEnabled = internalTimerEnabled;
        ref = new ConfigurationMethodRef();
        cepConfig.getEngineDefaults().getThreading().setInternalTimerEnabled(internalTimerEnabled);
        log.info("Added [" + eventType + "] as TEvent");
        cepConfig.addEventType("TEvent", eventType);
        cepConfig.getEngineDefaults().getLogging().setEnableTimerDebug(true);
        cep = EPServiceProviderManager.getProvider(provider, cepConfig);
        cepAdm = cep.getEPAdministrator();
        cepRT = cep.getEPRuntime();
    }

    public void setStreamEncoding(String encoding, BaselineStimulus eventType) {
        log.info("Added [" + eventType + "] as " + encoding);
        cepConfig.addEventType(encoding, eventType);
    }

    public void startProcessing() {
        //TODO put 0 1406872790001L
        cepRT.sendEvent(new CurrentTimeEvent(t0));
    }

    public void stopProcessing() {
        log.info("Engine is closing");
        //stop the CEP engine
        for (String stmtName : cepAdm.getStatementNames()) {
            EPStatement stmt = cepAdm.getStatement(stmtName);
            if (!stmt.isStopped()) {
                stmt.stop();
            }
        }
    }

    public ContinousQueryExecution registerQuery(Query q) {
        BaselineQuery bq = (BaselineQuery) q;
        Dataset dataset = DatasetFactory.create();
        JenaListener listener = new JenaListener(dataset, receiver, bq, reasoning, ontology_language, "http://streamreasoning.org/heaven/" + bq.getId());


        for (String[] pair : bq.getEsperNamedStreams()) {
            log.info("create named schema " + pair[1] + "() inherits TEvent");
            cepAdm.createEPL("create schema " + pair[1] + "() inherits TEvent");
            log.info("creating named graph " + pair[0] + "");
            if (!listener.addNamedWindowStream(pair[0])) {
                throw new StreamRegistrationException("Impossible to register window named  [" + pair[0] + "] on stream [" + pair[1] + "]");
            }
        }

        for (String c : bq.getEsperStreams()) {
            log.info("create schema " + c + "() inherits TEvent");
            cepAdm.createEPL("create schema " + c + "() inherits TEvent");
            if (!listener.addDefaultWindowStream(c)) {
                throw new StreamRegistrationException("Impossible to register stream [" + c + "]");
            }
        }


        for (String eq : bq.getEsper_queries()) {
            log.info("Register esper query [" + eq + "]");
            EPStatement epl = cepAdm.createEPL(eq);
            log.info("Add listener");
            epl.addListener(listener);
        }

        queries.put(q, listener);
        return new JenaCQueryExecution(dataset, listener);
    }

    public void registerReceiver(javax.sound.midi.Receiver receiver) {

    }
}
