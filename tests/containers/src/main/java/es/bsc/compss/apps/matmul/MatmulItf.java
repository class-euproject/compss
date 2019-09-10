package es.bsc.compss.apps.matmul;

import es.bsc.compss.types.annotations.Parameter;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Type;
import es.bsc.compss.types.annotations.task.Method;

public interface MatmulItf {

    @Method(declaringClass = "es.bsc.compss.apps.matmul.MatmulImpl")
    void multiplyAccumulative(
            @Parameter(type = Type.FILE, direction = Direction.INOUT) String file1,
            @Parameter(type = Type.FILE, direction = Direction.IN) String file2,
            @Parameter(type = Type.FILE, direction = Direction.IN) String file3,
            @Parameter int bsize
    ) throws Exception;

    @Method(declaringClass = "matmul.files.Block")
    void initBlockFile(
            @Parameter(type = Type.FILE, direction = Direction.OUT) String fileName,
            @Parameter int BSIZE,
            @Parameter boolean initRand
    );

}
