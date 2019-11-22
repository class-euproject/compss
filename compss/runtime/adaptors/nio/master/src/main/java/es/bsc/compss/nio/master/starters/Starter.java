package es.bsc.compss.nio.master.starters;

import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.commands.CommandCheckWorker;
import es.bsc.compss.nio.master.NIOAdaptor;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.handlers.ProcessOut;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.util.Tracer;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class Starter {

    // Static Environment variables
    protected static final String LIB_SEPARATOR = ":";
    protected static final String CLASSPATH_FROM_ENVIRONMENT = (System.getProperty(COMPSsConstants.WORKER_CP) != null
        && !System.getProperty(COMPSsConstants.WORKER_CP).isEmpty()) ? System.getProperty(COMPSsConstants.WORKER_CP)
            : "";

    protected static final String PYTHONPATH_FROM_ENVIRONMENT = (System.getProperty(COMPSsConstants.WORKER_PP) != null
        && !System.getProperty(COMPSsConstants.WORKER_PP).isEmpty()) ? System.getProperty(COMPSsConstants.WORKER_PP)
            : "";

    protected static final String LIBPATH_FROM_ENVIRONMENT = (System.getenv(COMPSsConstants.LD_LIBRARY_PATH) != null
        && !System.getenv(COMPSsConstants.LD_LIBRARY_PATH).isEmpty()) ? System.getenv(COMPSsConstants.LD_LIBRARY_PATH)
            : "";

    protected static final boolean IS_CPU_AFFINITY_DEFINED =
        System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY) != null
            && !System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY).isEmpty();
    protected static final String CPU_AFFINITY =
        IS_CPU_AFFINITY_DEFINED ? System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY)
            : ThreadBinder.BINDER_DISABLED;

    protected static final boolean IS_GPU_AFFINITY_DEFINED =
        System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY) != null
            && !System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY).isEmpty();
    protected static final String GPU_AFFINITY =
        IS_GPU_AFFINITY_DEFINED ? System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY)
            : ThreadBinder.BINDER_DISABLED;

    protected static final boolean IS_FPGA_AFFINITY_DEFINED =
        System.getProperty(COMPSsConstants.WORKER_FPGA_AFFINITY) != null
            && !System.getProperty(COMPSsConstants.WORKER_FPGA_AFFINITY).isEmpty();
    protected static final String FPGA_AFFINITY =
        IS_FPGA_AFFINITY_DEFINED ? System.getProperty(COMPSsConstants.WORKER_FPGA_AFFINITY)
            : ThreadBinder.BINDER_DISABLED;

    protected static final String WORKER_APPDIR_FROM_ENVIRONMENT =
        System.getProperty(COMPSsConstants.WORKER_APPDIR) != null
            && !System.getProperty(COMPSsConstants.WORKER_APPDIR).isEmpty()
                ? System.getProperty(COMPSsConstants.WORKER_APPDIR)
                : "";

    // Deployment ID
    protected static final String DEPLOYMENT_ID = System.getProperty(COMPSsConstants.DEPLOYMENT_ID);

    // Master name
    protected static final String MASTER_NAME_PROPERTY = System.getProperty(COMPSsConstants.MASTER_NAME);

    // Scripts configuration
    protected static final String STARTER_SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator
        + "system" + File.separator + "adaptors" + File.separator + "nio" + File.separator;
    protected static final String STARTER_SCRIPT_NAME = "persistent_worker.sh";

    protected static final String CLEAN_SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system"
        + File.separator + "adaptors" + File.separator + "nio" + File.separator;
    protected static final String CLEAN_SCRIPT_NAME = "persistent_worker_clean.sh";
    // Connection related parameters
    protected static final long START_WORKER_INITIAL_WAIT = 100;
    protected static final long WAIT_TIME_UNIT = 500;
    protected static final long MAX_WAIT_FOR_SSH = 160_000;
    protected static final long MAX_WAIT_FOR_INIT = 20_000;
    protected static final String ERROR_SHUTTING_DOWN_RETRY = "ERROR: Cannot shutdown failed worker PID process";

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    protected static final Map<String, Starter> addressToWorkerStarter = new TreeMap<>();
    protected boolean workerIsReady = false;
    protected boolean toStop = false;
    protected final NIOWorkerNode nw;


    /**
     * Instantiates a new WorkerStarter for a given Worker.
     *
     * @param nw Worker Node to start
     */
    public Starter(NIOWorkerNode nw) {
        this.nw = nw;
    }

    /**
     * Returns the WorkerStarter registered to a given address.
     *
     * @param address Address of the node
     * @return The starter for the node with the given address
     */
    public static Starter getWorkerStarter(String address) {
        return addressToWorkerStarter.get(address);
    }

    /**
     * Marks the worker as ready.
     */
    public void setWorkerIsReady() {
        LOGGER.debug("[WorkerStarter] Worker " + nw.getName() + " set to ready.");
        this.workerIsReady = true;
    }

    /**
     * Marks the worker to be stopped.
     */
    public void setToStop() {
        this.toStop = true;
    }

    protected void checkWorker(NIONode n, String name) {
        int delay = 300;
        int tries = 50;
        CommandCheckWorker cmd = new CommandCheckWorker(DEPLOYMENT_ID, name);
        do {

            if (DEBUG) {
                LOGGER.debug("[WorkerStarter] Sending check command to worker " + name);
            }

            // Send command check
            Connection c = NIOAdaptor.getTransferManager().startConnection(n);
            c.sendCommand(cmd);
            c.receive();
            c.finishConnection();

            // Sleep before next iteration
            try {
                LOGGER.debug("[WorkerStarter] Waiting to send next check worker command with delay " + 300);
                Thread.sleep(delay);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            delay = 300 + ((50 - tries) / 10) * 100;
        } while (!this.workerIsReady && --tries > 0 && !this.toStop);
    }

    // Arguments needed for persistent_worker.sh
    protected abstract String[] getStartCommand(int workerPort, String masterName) throws InitNodeException;

    protected ProcessOut runRemoteCommand(List<String> cmd) {
        try {
            final ProcessOut processOut = new ProcessOut();
            ProcessBuilder pb = new ProcessBuilder();
            pb.environment().remove(Tracer.LD_PRELOAD);
            pb.command(cmd);

            long start = System.currentTimeMillis();
            Process process = pb.start();

            final InputStream stderr = process.getErrorStream();
            final InputStream stdout = process.getInputStream();

            process.getOutputStream().close();

            process.waitFor();
            long end = System.currentTimeMillis();
            LOGGER.debug("The script ran for " + (end - start) + " milliseconds");
            processOut.setExitValue(process.exitValue());

            LOGGER.debug("Worker creation logs for " + this.nw.getName());
            BufferedReader reader = new BufferedReader(new InputStreamReader(stdout));
            String line;
            while ((line = reader.readLine()) != null) {
                processOut.appendOutput(line);
                LOGGER.debug("COMM CMD OUT: " + line);
            }
            reader = new BufferedReader(new InputStreamReader(stderr));
            while ((line = reader.readLine()) != null) {
                processOut.appendError(line);
                LOGGER.debug("COMM CMD ERR: " + line);
            }
            return processOut;
        } catch (Exception e) {
            LOGGER.error("Exception initializing worker ", e);
            return null;
        }
    }

    /**
     * Ender function called from the JVM Ender Hook.
     *
     * @param node Worker node.
     * @param pid Process PID.
     */
    public void ender(NIOWorkerNode node, int pid) {
        if (pid > 0 || (pid == -1 && this instanceof ContainerStarter)) {
            String user = node.getUser();

            // Clean worker working directory
            String jvmWorkerOpts = System.getProperty(COMPSsConstants.WORKER_JVM_OPTS);
            String removeWDFlagDisabled = COMPSsConstants.WORKER_REMOVE_WD + "=false";
            if ((jvmWorkerOpts != null && jvmWorkerOpts.contains(removeWDFlagDisabled))
                || this instanceof DockerStarter) {
                // User requested not to clean workers WD
                LOGGER.warn("RemoveWD set to false. Not Cleaning " + node.getName() + " working directory");
            } else {
                // Regular clean up
                String sandboxWorkingDir = node.getWorkingDir();
                String[] command = getCleanWorkerWorkingDir(sandboxWorkingDir);
                if (command != null) {
                    executeCommand(user, node.getName(), command);
                }
            }

            // Execute stop command
            String[] command = getStopCommand(pid);
            if (command.length > 0) {
                LOGGER.debug("getStopCommand generated this: " + Arrays.asList(command));
                executeCommand(user, node.getName(), command);
            }

        }
    }

    protected abstract String[] getStopCommand(int pid);

    protected ProcessOut executeCommand(String user, String resource, String[] command) {
        ProcessOut processOut = new ProcessOut();
        String[] cmd = this.nw.getConfiguration().getRemoteExecutionCommand(user, resource, command);
        if (cmd == null) {
            LOGGER.warn("Worker configured to be started by queue system.");
            return null;
        }
        // Log command
        StringBuilder sb = new StringBuilder("");
        for (String param : cmd) {
            sb.append(param).append(" ");
        }
        LOGGER.debug("COMM CMD: " + sb.toString());

        // Execute command
        return this.runRemoteCommand(Arrays.asList(cmd));
    }

    protected String[] getCleanWorkerWorkingDir(String workingDir) {
        String[] cmd = new String[3];
        // Send SIGTERM to allow ShutdownHooks on Worker
        cmd[0] = "rm";
        cmd[1] = "-rf";
        cmd[2] = workingDir;
        return cmd;
    }

    public abstract NIONode startWorker() throws InitNodeException;

    protected abstract void killPreviousWorker(String user, String name, int pid) throws InitNodeException;

}
