package es.bsc.compss.nio.master.starters;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.master.NIOAdaptor;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.handlers.Ender;
import es.bsc.compss.util.Tracer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public abstract class ContainerStarter extends Starter {

    private static final String DEFAULT_CONTAINER_APP_DIR = "/compss";
    private static final String NIO_WORKER_CLASS_NAME = "es.bsc.compss.nio.worker.NIOWorker";


    /**
     * Generic starter for workers running over containers.
     *
     * @param nw Worker node on which to run
     */
    public ContainerStarter(NIOWorkerNode nw) {
        super(nw);
    }

    @Override
    protected void killPreviousWorker(String user, String name, int pid) throws InitNodeException {

    }

    @Override
    protected String[] getStartCommand(int workerPort, String masterName) throws InitNodeException {
        String workerLibPath = "";
        String libPathFromFile = nw.getLibPath();
        if (!libPathFromFile.isEmpty()) {
            if (!LIBPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerLibPath = libPathFromFile + LIB_SEPARATOR + LIBPATH_FROM_ENVIRONMENT;
            } else {
                workerLibPath = libPathFromFile;
            }
        } else {
            workerLibPath = LIBPATH_FROM_ENVIRONMENT;
        }

        String appDir = nw.getAppDir();
        if (appDir == null || appDir.isEmpty()) {
            appDir = DEFAULT_CONTAINER_APP_DIR;
        }

        String workerClasspath = "";
        String classpathFromFile = nw.getClasspath();
        if (!classpathFromFile.isEmpty()) {
            if (!CLASSPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerClasspath = classpathFromFile + LIB_SEPARATOR + CLASSPATH_FROM_ENVIRONMENT;
            } else {
                workerClasspath = classpathFromFile;
            }
        } else {
            String[] paths = CLASSPATH_FROM_ENVIRONMENT.split(LIB_SEPARATOR);
            String jarName = paths[1].split("/")[paths[1].split("/").length - 1];
            workerClasspath = appDir + LIB_SEPARATOR + appDir + "/" + jarName + LIB_SEPARATOR
                + String.join(LIB_SEPARATOR, Arrays.copyOfRange(paths, 2, paths.length));
        }
        workerClasspath += ":" + nw.getInstallDir() + "/Runtime/adaptors/nio/worker/compss-adaptors-nio-worker.jar:"
            + nw.getInstallDir() + "/Runtime/compss-engine.jar";

        // Get JVM Flags
        String workerJVMflags = System.getProperty(COMPSsConstants.WORKER_JVM_OPTS);
        String[] jvmFlags = new String[0];
        if (workerJVMflags != null && !workerJVMflags.isEmpty()) {
            jvmFlags = workerJVMflags.split(",");
        }

        String workerFPGAargs = System.getProperty(COMPSsConstants.WORKER_FPGA_REPROGRAM);
        String[] fpgaArgs = new String[0];
        if (workerFPGAargs != null && !workerFPGAargs.isEmpty()) {
            fpgaArgs = workerFPGAargs.split(" ");
        }

        boolean debug = LOGGER.isDebugEnabled();
        final String itlog4jFile = debug ? "COMPSsWorker-log4j.debug" : "COMPSsWorker-log4j.off";

        final int maxSnd = NIOAdaptor.MAX_SEND_WORKER;
        final int maxRcv = NIOAdaptor.MAX_RECEIVE_WORKER;

        final String hostname = nw.getName();

        // workerPort
        // masterName

        final int masterPort = NIOAdaptor.MASTER_PORT;

        final int cpuComputingUnits = nw.getTotalComputingUnits();
        final int gpuComputingUnits = nw.getTotalGPUs();
        final int fpgaComputingUnits = nw.getTotalFPGAs();

        final String cpuMap = "disabled";
        final String gpuMap = GPU_AFFINITY;
        final String fpgaMap = FPGA_AFFINITY;

        final int limitOfTasks = nw.getLimitOfTasks();

        final String uuid = DEPLOYMENT_ID;
        final String lang = System.getProperty(COMPSsConstants.LANG);
        final String workingDir = nw.getWorkingDir();
        final String installDir = nw.getInstallDir();
        // appDir
        // workerLibPath
        // classpath

        String workerPythonpath = "";
        String pythonpathFromFile = nw.getPythonpath();
        if (!pythonpathFromFile.isEmpty()) {
            if (!PYTHONPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerPythonpath = pythonpathFromFile + LIB_SEPARATOR + PYTHONPATH_FROM_ENVIRONMENT;
            } else {
                workerPythonpath = pythonpathFromFile;
            }
        } else {
            workerPythonpath = PYTHONPATH_FROM_ENVIRONMENT;
        }

        final int traceLevel = NIOTracer.getLevel();
        final String extraeFile = NIOTracer.getExtraeFile();
        String extraeHostId;
        if (Tracer.isActivated()) {
            // NumSlots per host is ignored --> 0
            Integer hostId = NIOTracer.registerHost(nw.getName(), 0);
            extraeHostId = String.valueOf(hostId.toString());
        } else {
            extraeHostId = "NoTracinghostID";
        }

        String storageConf = System.getProperty(COMPSsConstants.STORAGE_CONF);
        if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
            storageConf = "null";
            LOGGER.warn("No storage configuration file passed");
        }
        String executionType = System.getProperty(COMPSsConstants.TASK_EXECUTION);
        if (executionType == null || executionType.equals("") || executionType.equals("null")) {
            executionType = es.bsc.compss.COMPSsConstants.TaskExecution.COMPSS.toString();
            LOGGER.warn("No executionType passed");
        }
        String isPersistent = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (isPersistent == null || isPersistent.isEmpty() || isPersistent.equals("null")) {
            isPersistent = COMPSsConstants.DEFAULT_PERSISTENT_C;
            LOGGER.warn("No persistent c passed");
        }

        String pythonInterpreter = System.getProperty(COMPSsConstants.PYTHON_INTERPRETER);
        if (pythonInterpreter == null || pythonInterpreter.isEmpty() || pythonInterpreter.equals("null")) {
            pythonInterpreter = COMPSsConstants.DEFAULT_PYTHON_INTERPRETER;
            LOGGER.warn("No python interpreter passed");
        }
        String pythonVersion = System.getProperty(COMPSsConstants.PYTHON_VERSION);
        if (pythonVersion == null || pythonVersion.isEmpty() || pythonVersion.equals("null")) {
            pythonVersion = COMPSsConstants.DEFAULT_PYTHON_VERSION;
            LOGGER.warn("No python version passed");
        }
        String pythonVirtualEnvironment = System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT);
        if (pythonVirtualEnvironment == null || pythonVirtualEnvironment.isEmpty()
            || pythonVirtualEnvironment.equals("null")) {
            pythonVirtualEnvironment = COMPSsConstants.DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT;
            LOGGER.warn("No python virtual environment passed");
        }
        String pythonPropagateVirtualEnvironment =
            System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
        if (pythonPropagateVirtualEnvironment == null || pythonPropagateVirtualEnvironment.isEmpty()
            || pythonPropagateVirtualEnvironment.equals("null")) {
            pythonPropagateVirtualEnvironment = COMPSsConstants.DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT;
            LOGGER.warn("No python propagate virtual environment passed");
        }
        String pythonMpiWorker = System.getProperty(COMPSsConstants.PYTHON_MPI_WORKER);
        if (pythonMpiWorker == null || pythonMpiWorker.isEmpty() || pythonMpiWorker.equals("null")) {
            pythonMpiWorker = COMPSsConstants.DEFAULT_PYTHON_MPI_WORKER;
            LOGGER.warn("No python MPI worker flag passed.");
        }

        List<String> cmd = new ArrayList<>();

        cmd.add("/usr/bin/java");
        cmd.addAll(Arrays.asList(jvmFlags));
        cmd.addAll(Arrays.asList("-XX:+PerfDisableSharedMem", "-XX:-UsePerfData", "-XX:+UseG1GC",
            "-XX:+UseThreadPriorities", "-XX:ThreadPriorityPolicy=42",
            "-Dlog4j.configurationFile=" + installDir + "/Runtime/configuration/log/" + itlog4jFile,
            "-Dcompss.python.interpreter=" + pythonInterpreter, "-Dcompss.python.version=" + pythonVersion,
            "-Dcompss.python.virtualenvironment=" + pythonVirtualEnvironment,
            "-Dcompss.python.propagate_virtualenvironment=" + pythonPropagateVirtualEnvironment,
            "-Dcompss.worker.removeWD=false", "-Djava.library.path=" + workerLibPath));
        cmd.addAll(Arrays.asList("-cp", workerClasspath));
        cmd.add(NIO_WORKER_CLASS_NAME);
        cmd.add(Boolean.toString(debug)); // 0
        cmd.add(Integer.toString(maxSnd)); // 1
        cmd.add(Integer.toString(maxRcv)); // 2
        cmd.add(hostname); // 3
        cmd.add(Integer.toString(workerPort)); // 4
        cmd.add(masterName); // 5
        cmd.add(Integer.toString(masterPort)); // 6
        cmd.add(Integer.toString(Comm.getStreamingPort())); // 7
        cmd.add(Integer.toString(cpuComputingUnits)); // 8
        cmd.add(Integer.toString(gpuComputingUnits)); // 9
        cmd.add(Integer.toString(fpgaComputingUnits)); // 10
        cmd.add(cpuMap); // 11
        cmd.add(gpuMap); // 12
        cmd.add(fpgaMap); // 13
        cmd.add(Integer.toString(limitOfTasks)); // 14
        cmd.add(uuid); // 15
        cmd.add(lang); // 16
        cmd.add(workingDir); // 17
        cmd.add(installDir); // 18
        cmd.add(appDir.isEmpty() ? "null" : appDir); // 19
        cmd.add(workerLibPath.isEmpty() ? "null" : workerLibPath); // 20
        cmd.add(workerClasspath.isEmpty() ? "null" : workerClasspath); // 21
        cmd.add(workerPythonpath.isEmpty() ? "null" : workerPythonpath); // 22
        cmd.add(Integer.toString(traceLevel)); // 23
        cmd.add(extraeFile); // 24
        cmd.add(extraeHostId); // 25
        cmd.add(storageConf); // 26
        cmd.add(executionType); // 27
        cmd.add(isPersistent); // 28
        cmd.add(pythonInterpreter); // 29
        cmd.add(pythonVersion); // 30
        cmd.add(pythonVirtualEnvironment); // 31
        cmd.add(pythonPropagateVirtualEnvironment); // 32
        cmd.add(pythonMpiWorker); // 33

        return cmd.toArray(new String[0]);
    }

    @Override
    public NIONode startWorker() throws InitNodeException {
        String name = this.nw.getName();
        String user = this.nw.getUser();

        int minPort = this.nw.getConfiguration().getMinPort();
        int maxPort = this.nw.getConfiguration().getMaxPort();

        String masterName = "null";
        if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals(""))
            && (!MASTER_NAME_PROPERTY.equals("null"))) {
            // Set the hostname from the defined property
            masterName = MASTER_NAME_PROPERTY;
        }

        synchronized (addressToWorkerStarter) {
            addressToWorkerStarter.put(name, this);
            LOGGER.debug("[ContainerStarter] Container starter for " + name + " registers in the hashmap");
        }

        String containerId = "";
        // String [] cmd = getStartCommand(minPort, masterName);
        // LOGGER.info("Starting container with command: " + String.join(" ", cmd));

        NIONode n = this.distribute(masterName, minPort, maxPort);
        // executeCommand(user, name, getStartCommand(port, masterName));
        checkWorker(n, name);

        if (!this.workerIsReady) {
            LOGGER.info("Tried to create container with range " + minPort + "-" + maxPort + " but failed.");
        } else {
            LOGGER.info("Container created successfully with port " + n.getPort());
            Runtime.getRuntime().addShutdownHook(new Ender(this, this.nw, -1));
            // container.setContainerId(containerId);
            return n;
        }

        String msg;

        if (this.toStop) {
            msg = "[STOP] Worker " + name + " stopped during creating because the app stopped";
        } else if (!this.workerIsReady) {
            msg = "[TIMEOUT] Could not start the NIO worker on resource " + name;
        } else {
            msg = "[UNKNOWN] Could not start the NIO worker on resource " + name;
        }
        LOGGER.error(msg);
        throw new InitNodeException(msg);
    }

    protected abstract NIONode distribute(String master, Integer minPort, Integer maxPort) throws InitNodeException;
}
