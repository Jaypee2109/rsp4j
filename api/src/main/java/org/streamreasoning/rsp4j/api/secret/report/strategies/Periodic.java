package org.streamreasoning.rsp4j.api.secret.report.strategies;

import org.streamreasoning.rsp4j.api.secret.content.Content;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.instance.Window;
import lombok.Setter;

/**
 * Periodic (Rpr): reporting is done for t only
 * if it is a multiple of x, where x denotes the reporting frequency.
 **/

//TODO returns true independently from w, but it
// communicates directly with the system clock to decide
// whether it is time to report
public class Periodic implements ReportingStrategy {
    @Setter
    private long period;

    @Override
    public boolean match(Window w, Content c, long tapp, long tsys) {
        return tapp % period == 0;
    }

}
