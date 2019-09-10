package es.bsc.compss.apps.xsimple;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.SchedulerHints;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;

public interface SimpleItf {

    @Method(declaringClass = "es.bsc.compss.apps.xsimple.SimpleImpl")
    //@Constraints(appSoftware="centosII")
    @SchedulerHints(isReplicated = "true")
    void increment(@Parameter(type = Type.OBJECT, direction = Direction.IN) Integer n);

}