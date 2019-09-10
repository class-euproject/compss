package es.bsc.compss.apps.increment;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;

public interface IncrementItf {

    @Method(declaringClass = "es.bsc.compss.apps.increment.IncrementImpl")
    void increment(@Parameter(type = Type.FILE, direction = Direction.INOUT) String file);

}
