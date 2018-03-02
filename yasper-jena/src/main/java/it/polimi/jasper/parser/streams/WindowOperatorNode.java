package it.polimi.jasper.parser.streams;

import it.polimi.yasper.core.spe.windowing.WindowOperator;
import it.polimi.yasper.core.enums.WindowType;

/**
 * Created by riccardo on 05/09/2017.
 */
public interface WindowOperatorNode extends WindowOperator{

    WindowType getType();

    int getT0();

    int getRange();

    int getStep();

    String getUnitRange();

    String getUnitStep();

}
