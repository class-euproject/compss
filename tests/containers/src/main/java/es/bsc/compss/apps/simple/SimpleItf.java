package es.bsc.compss.apps.simple;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.SchedulerHints;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;

public interface SimpleItf {

    @Method(declaringClass = "es.bsc.compss.apps.simple.SimpleImpl")
    void increment(@Parameter(type = Type.FILE, direction = Direction.INOUT) String file);

}