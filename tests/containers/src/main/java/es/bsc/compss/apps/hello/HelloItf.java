package es.bsc.compss.apps.hello;

import es.bsc.compss.types.annotations.task.Method;

public interface HelloItf {

    @Method(declaringClass = "es.bsc.compss.apps.hello.HelloImpl")
    void sayHello();

}
