package es.bsc.compss.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.master.NIOAdaptor;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.configuration.NIOConfiguration;
import es.bsc.compss.nio.master.configuration.rotterdam.config.ContainerConfig;
import es.bsc.compss.nio.master.configuration.rotterdam.config.QosConfig;
import es.bsc.compss.nio.master.configuration.rotterdam.config.RotterdamTaskDefinition;
import es.bsc.compss.nio.master.configuration.rotterdam.response.RotterdamTaskCreateResponse;
import es.bsc.compss.nio.master.configuration.rotterdam.response.RotterdamTaskRequestStatus;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class Rotterdam2Starter {

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

    private static final String DEFAULT_CONTAINER_APP_DIR = "/compss";
    private static final String NIO_WORKER_CLASS_NAME = "es.bsc.compss.nio.worker.NIOWorker";

    protected static String INFERRED_MASTER_NAME = null;

    private static final String IP_REGEX = "^(?=.*[^\\.]$)((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.?){4}$";

    private static final String TASK_CREATE_URL = "/api/v1/docks/tasks-compss";
    private static final String TASK_CHECK_TEMPLATE = "/api/v1/docks/%s/tasks/%s";
    private static final String SERVER_BASE = "http://rotterdam-caas.192.168.7.28.xip.io";

    private CloseableHttpClient httpClient;

    private MethodConfiguration methodConfiguration;
    private CommAdaptor adaptor;

    private String imageName;
    private int replicas;


    public Rotterdam2Starter(MethodConfiguration config, CommAdaptor adaptor) {
        this.methodConfiguration = config;
        this.adaptor = adaptor;

        this.httpClient = HttpClients.createDefault();

        this.imageName = config.getProperty("ImageName");
        this.replicas = Integer.parseInt(config.getProperty("Replicas"));
    }

    private boolean isSameNetwork(String[] ip1, String[] ip2, int amount) {
        if (amount == 0) {
            return true;
        }
        return ip1[0].equals(ip2[0]) && isSameNetwork(Arrays.copyOfRange(ip1, 1, ip1.length),
            Arrays.copyOfRange(ip2, 1, ip2.length), amount - 1);
    }

    private synchronized String inferMasterAddress() {
        if (INFERRED_MASTER_NAME != null) {
            return INFERRED_MASTER_NAME;
        }
        try {
            String[] workerIp = methodConfiguration.getHost().split("\\.");
            Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface iface : Collections.list(ifaces)) {
                Iterator<InterfaceAddress> addrIt = iface.getInterfaceAddresses().listIterator();
                while (addrIt.hasNext()) {
                    InterfaceAddress addr = addrIt.next();
                    if (addr.getAddress().getHostAddress().matches(IP_REGEX)) {
                        String[] ifaceIp = addr.getAddress().getHostAddress().split("\\.");
                        if (isSameNetwork(ifaceIp, workerIp, addr.getNetworkPrefixLength() / 8)) {
                            INFERRED_MASTER_NAME = addr.getAddress().getHostAddress();
                            return INFERRED_MASTER_NAME;
                        }
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String[] getStartCommand(int workerPort, String masterName) throws InitNodeException {
        String workerLibPath = "";
        String libPathFromFile = methodConfiguration.getLibraryPath();
        if (!libPathFromFile.isEmpty()) {
            if (!LIBPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerLibPath = libPathFromFile + LIB_SEPARATOR + LIBPATH_FROM_ENVIRONMENT;
            } else {
                workerLibPath = libPathFromFile;
            }
        } else {
            workerLibPath = LIBPATH_FROM_ENVIRONMENT;
        }

        String appDir = methodConfiguration.getAppDir();
        if (appDir == null || appDir.isEmpty()) {
            appDir = DEFAULT_CONTAINER_APP_DIR;
        }

        String workerClasspath = "";
        String classpathFromFile = methodConfiguration.getClasspath();
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
        workerClasspath +=
            ":" + methodConfiguration.getInstallDir() + "/Runtime/adaptors/nio/worker/compss-adaptors-nio-worker.jar:"
                + methodConfiguration.getInstallDir() + "/Runtime/compss-engine.jar";

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

        // final String hostname = methodConfiguration.getName();

        // workerPort

        if ("null".equals(masterName)) {
            if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals(""))
                && (!MASTER_NAME_PROPERTY.equals("null"))) {
                // Set the hostname from the defined property
                masterName = MASTER_NAME_PROPERTY;
            } else {
                if (methodConfiguration.getHost().matches(IP_REGEX)
                    && !"127.0.0.1".equals(methodConfiguration.getHost())) {
                    masterName = inferMasterAddress();
                }
            }
        } else if ("".equals(masterName)) {
            masterName = "null";
        }

        final int masterPort = NIOAdaptor.MASTER_PORT;

        final int cpuComputingUnits = methodConfiguration.getTotalComputingUnits();
        final int gpuComputingUnits = methodConfiguration.getTotalGPUComputingUnits();
        final int fpgaComputingUnits = methodConfiguration.getTotalFPGAComputingUnits();

        final String cpuMap = "disabled";
        final String gpuMap = GPU_AFFINITY;
        final String fpgaMap = FPGA_AFFINITY;

        final int limitOfTasks = methodConfiguration.getLimitOfTasks();

        final String uuid = DEPLOYMENT_ID;
        final String lang = System.getProperty(COMPSsConstants.LANG);
        final String workingDir = methodConfiguration.getSandboxWorkingDir();
        final String installDir = methodConfiguration.getInstallDir();
        // appDir
        // workerLibPath
        // classpath

        String workerPythonpath = "";
        String pythonpathFromFile = methodConfiguration.getPythonpath();
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
        String extraeHostId = "NoTracinghostID";

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
        cmd.add("$(hostname)"); // 3
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

    private List<NIONode> distribute(String master, Integer minPort) throws InitNodeException {
        String dock = "class";
        // String name = this.imageToContainerName(this.imageName) + "-" + DEPLOYMENT_ID.split("-")[0];
        RotterdamTaskDefinition createConfig = new RotterdamTaskDefinition("compss-app", dock, this.replicas);

        methodConfiguration.getAdditionalProperties().entrySet().stream().filter(e -> e.getKey().startsWith("qos:"))
            .findFirst().map(e -> new QosConfig(e.getKey().replaceAll("qos:", ""), e.getValue()))
            .ifPresent(createConfig::setQos);

        ContainerConfig containerConfig = new ContainerConfig("compss-app", this.imageName);
        containerConfig.setCommand(Arrays.asList("/bin/sh", "-c"));
        containerConfig.setArgs(Arrays.asList("mkdir -p " + methodConfiguration.getSandboxWorkingDir() + "/jobs && "
            + "mkdir -p " + methodConfiguration.getSandboxWorkingDir() + "/log && "
            + String.join(" ", getStartCommand(43001, master))));

        // IntStream.range(minPort, maxPort).forEach(n -> containerConfig.addPort(n, n, "tcp"));
        containerConfig.addPort(43001, minPort, "tcp");

        // Gson g = new Gson();
        Gson g = new GsonBuilder().setPrettyPrinting().create();
        try {
            createConfig.addContainer(containerConfig);

            HttpPost request = new HttpPost(SERVER_BASE.concat(TASK_CREATE_URL));
            request.setEntity(new StringEntity(g.toJson(createConfig)));
            request.setHeader("Content-Type", "application/json");

            CloseableHttpResponse httpResponse = httpClient.execute(request);
            RotterdamTaskCreateResponse taskCreateResponse =
                g.fromJson(IOUtils.toString(httpResponse.getEntity().getContent()), RotterdamTaskCreateResponse.class);
            if (httpResponse.getStatusLine().getStatusCode() != 200 && "ok".equals(taskCreateResponse.getResp())) {
                throw new InitNodeException("Task could not be created");
            }
            httpResponse.close();

            String name = taskCreateResponse.getTask().getId();

            RotterdamTaskRequestStatus status;
            do {
                HttpGet taskCheckRequest =
                    new HttpGet(SERVER_BASE.concat(String.format(TASK_CHECK_TEMPLATE, dock, name)));
                CloseableHttpResponse response = httpClient.execute(taskCheckRequest);
                String responseJson = IOUtils.toString(response.getEntity().getContent(), "utf-8");
                status = g.fromJson(responseJson, RotterdamTaskRequestStatus.class);
                if (response.getStatusLine().getStatusCode() == 200 && "ok".equals(status.getResp())
                    && !status.getTask().getPods().isEmpty()) {
                    break;
                }
                Thread.sleep(1000);
            } while (true);
            return status.getTask().getPods().stream().map(pod -> new NIONode(pod.getIp(), pod.getPort()))
                .collect(Collectors.toList());
        } catch (Exception e) {
            throw new InitNodeException(e);
        }
    }

    public List<COMPSsWorker> create() throws InitNodeException {
        return distribute(MASTER_NAME_PROPERTY, 43001).stream()
            .map(n -> new NIOWorkerNode((NIOConfiguration) this.methodConfiguration, (NIOAdaptor) this.adaptor, n))
            .collect(Collectors.toList());
    }
}
