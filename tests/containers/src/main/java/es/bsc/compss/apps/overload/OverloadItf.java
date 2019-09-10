package es.bsc.compss.apps.overload;

import es.bsc.compss.types.annotations.task.Method;

public interface OverloadItf {

    @Method(declaringClass = "es.bsc.compss.apps.overload.OverloadImpl")
    void run();

}
