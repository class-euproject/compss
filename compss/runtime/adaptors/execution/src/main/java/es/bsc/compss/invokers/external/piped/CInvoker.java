/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.invokers.external.piped;

import es.bsc.compss.executor.ExecutorContext;
import es.bsc.compss.executor.external.ExecutionPlatformMirror;
import es.bsc.compss.executor.external.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.executor.external.piped.PipePair;
import es.bsc.compss.executor.external.piped.commands.ExecuteTaskPipeCommand;
import es.bsc.compss.executor.types.InvocationResources;
import es.bsc.compss.invokers.util.CExecutionCommandGenerator;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;

import java.io.File;
import java.util.ArrayList;


public class CInvoker extends PipedInvoker {

    public CInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
            InvocationResources assignedResources, PipePair pipes) throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources, pipes);

    }

    @Override
    protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation,
            String sandBox, InvocationResources assignedResources) {
        ExecuteTaskPipeCommand taskExecution = new ExecuteTaskPipeCommand(invocation.getJobId());
        ArrayList<String> cCommand = CExecutionCommandGenerator.getTaskExecutionCommand(context, invocation, sandBox,
                assignedResources);
        taskExecution.appendAllArguments(cCommand);
        return taskExecution;
    }

    public static ExecutionPlatformMirror<?> getMirror(InvocationContext context, ExecutorContext platform) {
        int numThreads = platform.getSize();
        return new CMirror(context, numThreads);
    }

}
