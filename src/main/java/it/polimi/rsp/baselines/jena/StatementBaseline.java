package it.polimi.rsp.baselines.jena;

import it.polimi.rsp.baselines.jena.events.stimuli.StatementStimulus;
import it.polimi.heaven.core.teststand.EventProcessor;
import it.polimi.heaven.core.teststand.rspengine.events.Response;
import it.polimi.heaven.core.teststand.rspengine.events.Stimulus;
import lombok.extern.log4j.Log4j;

import com.espertech.esper.client.time.CurrentTimeEvent;

@Log4j
public class StatementBaseline extends JenaEngine {

	public StatementBaseline(EventProcessor<Response> collector) {
		super(new StatementStimulus(), collector);
	}

	@Override
	public boolean process(Stimulus e) {
		this.currentEvent = e;
		StatementStimulus s = (StatementStimulus) e;
		cepRT.sendEvent(s, s.getStream_name());
		log.debug("Received Stimulus [" + s + "]");
		rspEventsNumber++;
		if (!this.internalTimerEnabled && currentTimestamp != s.getAppTimestamp()) {
			cepRT.sendEvent(new CurrentTimeEvent(currentTimestamp = s.getAppTimestamp()));
			log.debug("Sent time Event current runtime ts [" + currentTimestamp + "]");
		}
		return true;
	}

}
