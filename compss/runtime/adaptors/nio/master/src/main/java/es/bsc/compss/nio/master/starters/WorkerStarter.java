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
import es.bsc.compss.nio.master.NIOStarterCommand;
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

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);


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
        String[] command = generateStartCommand(port, masterName, "NoTracingHostID");
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

    // Arguments needed for persistent_worker.sh
    @Override
    protected String[] generateStartCommand(int workerPort, String masterName, String hostId) throws InitNodeException {
        final String workingDir = this.nw.getWorkingDir();
        final String installDir = this.nw.getInstallDir();
        final String appDir = this.nw.getAppDir();
        String classpathFromFile = this.nw.getClasspath();
        String pythonpathFromFile = this.nw.getPythonpath();
        String libPathFromFile = this.nw.getLibPath();
        String workerName = this.nw.getName();
        int totalCPU = this.nw.getTotalComputingUnits();
        int totalGPU = this.nw.getTotalGPUs();
        int totalFPGA = this.nw.getTotalFPGAs();
        int limitOfTasks = this.nw.getLimitOfTasks();

        try {
            return new NIOStarterCommand(workerName, workerPort, masterName, workingDir, installDir, appDir,
                classpathFromFile, pythonpathFromFile, libPathFromFile, totalCPU, totalGPU, totalFPGA, limitOfTasks,
                hostId, false).getStartCommand();
        } catch (Exception e) {
            throw new InitNodeException(e);
        }
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
