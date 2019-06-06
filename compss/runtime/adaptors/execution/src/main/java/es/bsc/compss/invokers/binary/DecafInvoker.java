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
package es.bsc.compss.invokers.binary;

import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.invokers.util.StdIOStream;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.DecafImplementation;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;


public class DecafInvoker extends Invoker {

    private static final int NUM_BASE_DECAF_ARGS = 11;

    private static final String ERROR_DECAF_RUNNER = "ERROR: Invalid mpiRunner";
    private static final String ERROR_DECAF_BINARY = "ERROR: Invalid wfScript";
    private static final String ERROR_TARGET_PARAM = "ERROR: MPI Execution doesn't support target parameters";

    private final String mpiRunner;
    private String dfScript;
    private String dfExecutor;
    private String dfLib;


    /**
     * Decaf Invoker constructor.
     * 
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param taskSandboxWorkingDir Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the Decaf invoker.
     */
    public DecafInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
            InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        // Get method definition properties
        DecafImplementation decafImpl = null;
        try {
            decafImpl = (DecafImplementation) invocation.getMethodImplementation();
        } catch (Exception e) {
            throw new JobExecutionException(
                    ERROR_METHOD_DEFINITION + invocation.getMethodImplementation().getMethodType(), e);
        }
        this.mpiRunner = decafImpl.getMpiRunner();
        this.dfScript = decafImpl.getDfScript();
        this.dfExecutor = decafImpl.getDfExecutor();
        this.dfLib = decafImpl.getDfLib();
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        checkArguments();

        LOGGER.info("Invoked " + this.dfScript + " in " + this.context.getHostName());

        // Execute binary
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }

        // Close out streams if any
        try {
            BinaryRunner.closeStreams(this.invocation.getParams(), this.jythonPycompssHome);
        } catch (StreamCloseException se) {
            LOGGER.error("Exception closing binary streams", se);
            throw new JobExecutionException(se);
        }

        // Update binary results
        for (InvocationParam np : this.invocation.getResults()) {
            if (np.getType() == DataType.FILE_T) {
                serializeBinaryExitValue(np, retValue);
            } else {
                np.setValue(retValue);
                np.setValueClass(retValue.getClass());
            }
        }
    }

    private void checkArguments() throws JobExecutionException {
        if (this.mpiRunner == null || this.mpiRunner.isEmpty()) {
            throw new JobExecutionException(ERROR_DECAF_RUNNER);
        }
        if (this.dfScript == null || this.dfScript.isEmpty()) {
            throw new JobExecutionException(ERROR_DECAF_BINARY);
        }
        if (!this.dfScript.startsWith(File.separator)) {
            this.dfScript = context.getAppDir() + File.separator + this.dfScript;
        }
        if (this.dfExecutor == null || this.dfExecutor.isEmpty() || this.dfExecutor.equals(Constants.UNASSIGNED)) {
            this.dfExecutor = "executor.sh";
        }
        if (!this.dfExecutor.startsWith(File.separator) && !this.dfExecutor.startsWith("./")) {
            this.dfExecutor = "./" + this.dfExecutor;
        }
        if (this.dfLib == null || this.dfLib.isEmpty()) {
            this.dfLib = "null";
        }
        if (invocation.getTarget() != null && this.invocation.getTarget().getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    private Object runInvocation() throws InvokeExecutionException {
        String dfRunner = this.context.getInstallDir() + DecafImplementation.SCRIPT_PATH;

        // Command similar to
        // export OMP_NUM_THREADS=1 ; mpirun -H COMPSsWorker01,COMPSsWorker02 -n
        // 2 (--bind-to core) exec args
        // Get COMPSS ENV VARS

        // Convert binary parameters and calculate binary-streams redirection
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
                this.invocation.getTarget(), streamValues, this.jythonPycompssHome);

        // Prepare command
        String args = new String();
        for (int i = 0; i < binaryParams.size(); ++i) {
            if (i == 0) {
                args = args.concat(binaryParams.get(i));
            } else {
                args = args.concat(" " + binaryParams.get(i));
            }
        }
        String[] cmd;
        if (args.isEmpty()) {
            cmd = new String[NUM_BASE_DECAF_ARGS - 2];
        } else {
            cmd = new String[NUM_BASE_DECAF_ARGS];
        }
        cmd[0] = dfRunner;
        cmd[1] = this.dfScript;
        cmd[2] = this.dfExecutor;
        cmd[3] = this.dfLib;
        cmd[4] = this.mpiRunner;

        String numProcs = String.valueOf(this.numWorkers * this.computingUnits);
        cmd[5] = "-n";
        cmd[6] = numProcs;

        String hostfile = writeHostfile(this.taskSandboxWorkingDir, this.workers);
        cmd[7] = "--hostfile";
        cmd[8] = hostfile;
        if (!args.isEmpty()) {
            cmd[9] = "--args=\"";
            cmd[10] = args;
        }

        // Prepare environment
        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = context.getThreadOutStream();
            outLog.println("");
            outLog.println("[DECAF INVOKER] Begin DECAF call to " + this.dfScript);
            outLog.println("[DECAF INVOKER] On WorkingDir : " + this.taskSandboxWorkingDir.getAbsolutePath());
            // Debug command
            outLog.print("[DECAF INVOKER] Decaf CMD: ");
            for (int i = 0; i < cmd.length; ++i) {
                outLog.print(cmd[i] + " ");
            }
            outLog.println("");
            outLog.println("[DECAF INVOKER] Decaf STDIN: " + streamValues.getStdIn());
            outLog.println("[DECAF INVOKER] Decaf STDOUT: " + streamValues.getStdOut());
            outLog.println("[DECAF INVOKER] Decaf STDERR: " + streamValues.getStdErr());
        }
        // Launch command
        return BinaryRunner.executeCMD(cmd, streamValues, this.taskSandboxWorkingDir, this.context.getThreadOutStream(),
                this.context.getThreadErrStream());
    }

}
