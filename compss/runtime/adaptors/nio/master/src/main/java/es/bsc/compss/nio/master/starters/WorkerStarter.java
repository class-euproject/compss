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
package es.bsc.compss.nio.master.starters;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.commands.CommandCheckWorker;
import es.bsc.compss.nio.master.NIOAdaptor;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.handlers.Ender;
import es.bsc.compss.nio.master.handlers.ProcessOut;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.util.Tracer;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WorkerStarter extends Starter {

    /**
     * Instantiates a new WorkerStarter for a given Worker.
     *
     * @param nw NIOWorkerNode to start
     */
    public WorkerStarter(NIOWorkerNode nw) {
        super(nw);
    }

    private int startWorker(String user, String name, int port, String masterName) throws InitNodeException {
        // Initial wait
        try {
            Thread.sleep(START_WORKER_INITIAL_WAIT);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        long timer = START_WORKER_INITIAL_WAIT;

        // Try to launch the worker until we receive the PID or we timeout
        int pid = -1;
        String[] command = getStartCommand(port, masterName);
        do {
            ProcessOut po = executeCommand(user, name, command);
            if (po == null) {
                // Queue System managed worker starter
                LOGGER.debug("Worker process started in resource " + name + " by queue system.");
                pid = 0;
            } else if (po.getExitValue() == 0) {
                // Success
                String output = po.getOutput();
                String[] lines = output.split("\n");
                pid = Integer.parseInt(lines[lines.length - 1]);
            } else {
                if (timer > MAX_WAIT_FOR_SSH) {
                    // Timeout
                    throw new InitNodeException(
                        "[START_CMD_ERROR]: Could not start the NIO worker in resource " + name + " through user "
                            + user + ".\n" + "OUTPUT:" + po.getOutput() + "\n" + "ERROR:" + po.getError() + "\n");
                }
                LOGGER.warn(" Worker process failed to start in resource " + name + ". Retrying...");
            }

            // Sleep between retries
            try {
                Thread.sleep(4 * WAIT_TIME_UNIT);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            timer = timer + (4 * WAIT_TIME_UNIT);
        } while (pid < 0);

        return pid;
    }

    /**
     * Starts the current worker.
     *
     * @return Started NIONode
     * @throws InitNodeException When the node could not be initialized
     */
    @Override
    public NIONode startWorker() throws InitNodeException {
        String name = this.nw.getName();
        String user = this.nw.getUser();
        int minPort = this.nw.getConfiguration().getMinPort();
        int maxPort = this.nw.getConfiguration().getMaxPort();
        int port = minPort;
        String masterName = "null";
        if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals(""))
            && (!MASTER_NAME_PROPERTY.equals("null"))) {
            // Set the hostname from the defined property
            masterName = MASTER_NAME_PROPERTY;
        }
        // Solves exit error 143
        synchronized (addressToWorkerStarter) {
            addressToWorkerStarter.put(name, this);
            LOGGER.debug("[WorkerStarter] Worker starter for " + name + " registers in the hashmap");
        }

        NIONode n = null;
        int pid = -1;
        while (port <= maxPort && !this.toStop) {
            // Kill previous worker processes if any
            killPreviousWorker(user, name, pid);

            // Instantiate the node
            n = new NIONode(name, port);

            // Start the worker
            pid = startWorker(user, name, port, masterName);

            // Check worker status
            LOGGER.info("[WorkerStarter] Worker process started. Checking connectivity...");
            checkWorker(n, name);

            // Check received ack
            LOGGER.debug("[WorkerStarter] Retries for " + name + " have finished.");
            if (!this.workerIsReady) {
                // Try next port
                ++port;
            } else {
                // Success, return node
                try {
                    Runtime.getRuntime().addShutdownHook(new Ender(this, this.nw, pid));
                } catch (IllegalStateException e) {
                    LOGGER.warn("Tried to shutdown vm while it was already being shutdown", e);
                }
                return n;
            }
        }

        // The loop has finished because there is no available node.
        // This can be because node is stopping or because we reached the maximum available ports
        if (this.toStop) {
            String msg = "[STOP]: Worker " + name + " stopped during creation because application is stopped";
            LOGGER.warn(msg);
            killPreviousWorker(user, name, pid);
            throw new InitNodeException(msg);
        } else if (!this.workerIsReady) {
            String msg =
                "[TIMEOUT]: Could not start the NIO worker on resource " + name + " through user " + user + ".";
            LOGGER.warn(msg);
            killPreviousWorker(user, name, pid);
            throw new InitNodeException(msg);
        } else {
            String msg =
                "[UNKNOWN]: Could not start the NIO worker on resource " + name + " through user " + user + ".";
            LOGGER.warn(msg);
            killPreviousWorker(user, name, pid);
            throw new InitNodeException(msg);
        }
    }

    @Override
    protected void killPreviousWorker(String user, String name, int pid) throws InitNodeException {
        if (pid != -1) {
            // Command was started but it is not possible to contact to the worker
            String[] command = getStopCommand(pid);
            ProcessOut po = executeCommand(user, name, command);
            if (po == null) {
                // Queue System managed worker starter
                LOGGER
                    .error("[START_CMD_ERROR]: An Error has occurred when queue system started NIO worker in resource "
                        + name + ". Retries not available in this option.");
                throw new InitNodeException(
                    "[START_CMD_ERROR]: An Error has occurred when queue system started NIO worker in resource " + name
                        + ". Retries not available in this option.");
            } else if (po.getExitValue() != 0) {
                // Normal starting process
                LOGGER.error(ERROR_SHUTTING_DOWN_RETRY);
            }
        }
    }

    @Override
    protected String[] getStartCommand(int workerPort, String masterName) throws InitNodeException {
        final String workingDir = this.nw.getWorkingDir();
        final String installDir = this.nw.getInstallDir();
        final String appDir = this.nw.getAppDir();

        // Merge command classpath and worker defined classpath
        String workerClasspath = "";
        String classpathFromFile = this.nw.getClasspath();
        if (!classpathFromFile.isEmpty()) {
            if (!CLASSPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerClasspath = classpathFromFile + LIB_SEPARATOR + CLASSPATH_FROM_ENVIRONMENT;
            } else {
                workerClasspath = classpathFromFile;
            }
        } else {
            workerClasspath = CLASSPATH_FROM_ENVIRONMENT;
        }

        // Merge command pythonpath and worker defined pythonpath
        String workerPythonpath = "";
        String pythonpathFromFile = this.nw.getPythonpath();
        if (!pythonpathFromFile.isEmpty()) {
            if (!PYTHONPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerPythonpath = pythonpathFromFile + LIB_SEPARATOR + PYTHONPATH_FROM_ENVIRONMENT;
            } else {
                workerPythonpath = pythonpathFromFile;
            }
        } else {
            workerPythonpath = PYTHONPATH_FROM_ENVIRONMENT;
        }

        // Merge command libpath and machine defined libpath
        String workerLibPath = "";
        String libPathFromFile = this.nw.getLibPath();
        if (!libPathFromFile.isEmpty()) {
            if (!LIBPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerLibPath = libPathFromFile + LIB_SEPARATOR + LIBPATH_FROM_ENVIRONMENT;
            } else {
                workerLibPath = libPathFromFile;
            }
        } else {
            workerLibPath = LIBPATH_FROM_ENVIRONMENT;
        }

        // Get JVM Flags
        String workerJVMflags = System.getProperty(COMPSsConstants.WORKER_JVM_OPTS);
        String[] jvmFlags = new String[0];
        if (workerJVMflags != null && !workerJVMflags.isEmpty()) {
            jvmFlags = workerJVMflags.split(",");
        }

        // Get FPGA reprogram args
        String workerFPGAargs = System.getProperty(COMPSsConstants.WORKER_FPGA_REPROGRAM);
        String[] fpgaArgs = new String[0];
        if (workerFPGAargs != null && !workerFPGAargs.isEmpty()) {
            fpgaArgs = workerFPGAargs.split(" ");
        }

        // Configure worker debug level
        final String workerDebug = Boolean.toString(LogManager.getLogger(Loggers.WORKER).isDebugEnabled());

        // Configure storage
        String storageConf = System.getProperty(COMPSsConstants.STORAGE_CONF);
        if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
            storageConf = "null";
        }
        String executionType = System.getProperty(COMPSsConstants.TASK_EXECUTION);
        if (executionType == null || executionType.equals("") || executionType.equals("null")) {
            executionType = COMPSsConstants.TaskExecution.COMPSS.toString();
        }

        // configure persistent_worker_c execution
        String workerPersistentC = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (workerPersistentC == null || workerPersistentC.isEmpty() || workerPersistentC.equals("null")) {
            workerPersistentC = COMPSsConstants.DEFAULT_PERSISTENT_C;
        }

        // Configure python interpreter
        String pythonInterpreter = System.getProperty(COMPSsConstants.PYTHON_INTERPRETER);
        if (pythonInterpreter == null || pythonInterpreter.isEmpty() || pythonInterpreter.equals("null")) {
            pythonInterpreter = COMPSsConstants.DEFAULT_PYTHON_INTERPRETER;
        }

        // Configure python version
        String pythonVersion = System.getProperty(COMPSsConstants.PYTHON_VERSION);
        if (pythonVersion == null || pythonVersion.isEmpty() || pythonVersion.equals("null")) {
            pythonVersion = COMPSsConstants.DEFAULT_PYTHON_VERSION;
        }

        // Configure python virtual environment
        String pythonVirtualEnvironment = System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT);
        if (pythonVirtualEnvironment == null || pythonVirtualEnvironment.isEmpty()
            || pythonVirtualEnvironment.equals("null")) {
            pythonVirtualEnvironment = COMPSsConstants.DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT;
        }
        String pythonPropagateVirtualEnvironment =
            System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
        if (pythonPropagateVirtualEnvironment == null || pythonPropagateVirtualEnvironment.isEmpty()
            || pythonPropagateVirtualEnvironment.equals("null")) {
            pythonPropagateVirtualEnvironment = COMPSsConstants.DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT;
        }

        String pythonMpiWorker = System.getProperty(COMPSsConstants.PYTHON_MPI_WORKER);
        if (pythonMpiWorker == null || pythonMpiWorker.isEmpty() || pythonMpiWorker.equals("null")) {
            pythonMpiWorker = COMPSsConstants.DEFAULT_PYTHON_MPI_WORKER;
        }

        /*
         * ************************************************************************************************************
         * BUILD COMMAND
         * ************************************************************************************************************
         */
        String[] cmd = new String[NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + NIOAdaptor.NUM_PARAMS_NIO_WORKER
            + jvmFlags.length + 1 + fpgaArgs.length];

        /* SCRIPT ************************************************ */
        cmd[0] = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + STARTER_SCRIPT_PATH
            + STARTER_SCRIPT_NAME; // 0 script directory

        /* Values ONLY for persistent_worker.sh ****************** */
        cmd[1] = workerLibPath.isEmpty() ? "null" : workerLibPath; // 1

        if (WORKER_APPDIR_FROM_ENVIRONMENT.isEmpty() && appDir.isEmpty()) {
            LOGGER.warn("No path passed via appdir option neither xml AppDir field");
            cmd[2] = "null"; // 2
        } else if (!appDir.isEmpty()) {
            if (!WORKER_APPDIR_FROM_ENVIRONMENT.isEmpty()) {
                LOGGER.warn("Path passed via appdir option and xml AppDir field."
                    + "The path provided by the xml will be used");
            }
            cmd[2] = appDir;
        } else if (!WORKER_APPDIR_FROM_ENVIRONMENT.isEmpty()) {
            cmd[2] = WORKER_APPDIR_FROM_ENVIRONMENT;
        }

        cmd[3] = workerClasspath.isEmpty() ? "null" : workerClasspath; // 3

        cmd[4] = Comm.getStreamingBackend().name();

        cmd[5] = String.valueOf(jvmFlags.length);
        for (int i = 0; i < jvmFlags.length; ++i) {
            cmd[NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + i] = jvmFlags[i];
        }

        int nextPosition = NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + jvmFlags.length;
        cmd[nextPosition++] = String.valueOf(fpgaArgs.length);
        for (String fpgaArg : fpgaArgs) {
            cmd[nextPosition++] = fpgaArg;
        }

        /* Values for NIOWorker ********************************** */
        cmd[nextPosition++] = workerDebug; // 0

        // Internal parameters
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_SEND_WORKER); // 1
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_RECEIVE_WORKER); // 2
        cmd[nextPosition++] = this.nw.getName(); // 3
        cmd[nextPosition++] = String.valueOf(workerPort); // 4
        cmd[nextPosition++] = masterName; // 5
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MASTER_PORT); // 6
        cmd[nextPosition++] = String.valueOf(Comm.getStreamingPort());

        // Worker parameters
        cmd[nextPosition++] = String.valueOf(this.nw.getTotalComputingUnits()); // 7
        cmd[nextPosition++] = String.valueOf(this.nw.getTotalGPUs()); // 8
        cmd[nextPosition++] = String.valueOf(this.nw.getTotalFPGAs()); // 9
        // get cpu_affinity from properties

        String cpuAffinity = nw.getConfiguration().getProperty("cpu_affinity"); // 10
        if (cpuAffinity != null) {
            cmd[nextPosition++] = String.valueOf(CPU_AFFINITY);
        } else {
            cmd[nextPosition++] = String.valueOf(CPU_AFFINITY);
        }
        cmd[nextPosition++] = String.valueOf(GPU_AFFINITY); // 11
        cmd[nextPosition++] = String.valueOf(FPGA_AFFINITY); // 12
        cmd[nextPosition++] = String.valueOf(this.nw.getLimitOfTasks()); // 13

        // Application parameters
        cmd[nextPosition++] = DEPLOYMENT_ID; // 14
        cmd[nextPosition++] = System.getProperty(COMPSsConstants.LANG); // 15
        cmd[nextPosition++] = workingDir; // 16
        cmd[nextPosition++] = this.nw.getInstallDir(); // 17

        cmd[nextPosition++] = cmd[2]; // 18
        cmd[nextPosition++] = workerLibPath.isEmpty() ? "null" : workerLibPath; // 19
        cmd[nextPosition++] = workerClasspath.isEmpty() ? "null" : workerClasspath; // 20
        cmd[nextPosition++] = workerPythonpath.isEmpty() ? "null" : workerPythonpath; // 21

        // Tracing parameters
        cmd[nextPosition++] = String.valueOf(NIOTracer.getLevel()); // 22
        cmd[nextPosition++] = NIOTracer.getExtraeFile(); // 23
        if (Tracer.extraeEnabled()) {
            // NumSlots per host is ignored --> 0
            Integer hostId = NIOTracer.registerHost(this.nw.getName(), 0);
            cmd[nextPosition++] = String.valueOf(hostId.toString()); // 24
        } else {
            cmd[nextPosition++] = "NoTracinghostID"; // 24
        }

        // Storage parameters
        cmd[nextPosition++] = storageConf; // 25
        cmd[nextPosition++] = executionType; // 26

        // persistent_c parameter
        cmd[nextPosition++] = workerPersistentC; // 27

        // Python interpreter parameter
        cmd[nextPosition++] = pythonInterpreter; // 28
        // Python interpreter version
        cmd[nextPosition++] = pythonVersion; // 29
        // Python virtual environment parameter
        cmd[nextPosition++] = pythonVirtualEnvironment; // 30
        // Python propagate virtual environment parameter
        cmd[nextPosition++] = pythonPropagateVirtualEnvironment; // 31
        // Python use MPI worker parameter
        cmd[nextPosition++] = pythonMpiWorker; // 32

        if (cmd.length != nextPosition) {
            throw new InitNodeException(
                "ERROR: Incorrect number of parameters. Expected: " + cmd.length + ". Got: " + nextPosition);
        }

        return cmd;

    }

    @Override
    protected String[] getStopCommand(int pid) {
        String[] cmd = new String[2];
        String installDir = this.nw.getInstallDir();

        // Send SIGTERM to allow ShutdownHooks on Worker...
        // Send SIGKILL to all child processes of 'pid'
        // and send a SIGTERM to the parent process
        // ps --ppid 2796 -o pid= | awk '{ print $1 }' | xargs kill -15 <--- kills all childs of ppid
        // kill -15 2796 kills the parentpid
        // necessary to check whether it has file separator or not? /COMPSs////Runtime == /COMPSs/Runtime in bash
        cmd[0] = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + CLEAN_SCRIPT_PATH
            + CLEAN_SCRIPT_NAME;
        cmd[1] = String.valueOf(pid);

        return cmd;
    }

}
