package es.bsc.compss.apps.easy;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;

public interface EasyItf {

    @Method(declaringClass="es.bsc.compss.apps.easy.EasyImpl")
    int getRandomNumber(@Parameter(type = Type.INT, direction = Direction.IN) int max);

    @Method(declaringClass="es.bsc.compss.apps.easy.EasyImpl")
    int sum(@Parameter(type=Type.INT, direction = Direction.IN) int a,
            @Parameter(type=Type.INT, direction = Direction.IN) int b);

}
