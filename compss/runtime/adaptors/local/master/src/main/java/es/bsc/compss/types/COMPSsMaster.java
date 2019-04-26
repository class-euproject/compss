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
package es.bsc.compss.types;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsConstants.TaskExecution;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.executor.ExecutionManager;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.types.ExecutionListener;
import es.bsc.compss.executor.utils.ThreadedPrintStream;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.invokers.types.JavaParams;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.local.LocalJob;
import es.bsc.compss.local.LocalParameter;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.types.data.location.DataLocation.Type;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.BindingDataManager;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import storage.StorageItf;


/**
 * Representation of the COMPSs Master Node Only 1 instance per execution
 */
public final class COMPSsMaster extends COMPSsWorker implements InvocationContext {

    private static final String ERROR_COMPSs_LOG_BASE_DIR = "ERROR: Cannot create .COMPSs base log directory";
    private static final String ERROR_APP_OVERLOAD = "ERROR: Cannot erase overloaded directory";
    private static final String ERROR_APP_LOG_DIR = "ERROR: Cannot create application log directory";
    private static final String ERROR_TEMP_DIR = "ERROR: Cannot create temp directory";
    private static final String ERROR_JOBS_DIR = "ERROR: Cannot create jobs directory";
    private static final String ERROR_WORKERS_DIR = "ERROR: Cannot create workers directory";
    private static final String WARN_FOLDER_OVERLOAD = "WARNING: Reached maximum number of executions for this application. To avoid this warning please clean .COMPSs folder";
    private static final String EXECUTION_MANAGER_ERR = "Error starting ExecutionManager";

    private static final int MAX_OVERLOAD = 100; // Maximum number of executions of same application
    public static final String SUFFIX_OUT = ".out";
    public static final String SUFFIX_ERR = ".err";

    private final String name;

    private final String storageConf;
    private final TaskExecution executionType;

    private final String userExecutionDirPath;
    private final String COMPSsLogBaseDirPath;
    private final String appLogDirPath;

    private final String installDirPath;
    private final String appDirPath;
    private final String tempDirPath;
    private final String jobsDirPath;
    private final String workersDirPath;

    private final LanguageParams[] langParams = new LanguageParams[COMPSsConstants.Lang.values().length];
    private boolean persistentEnabled;

    private ExecutionManager executionManager;
    private final ThreadedPrintStream out;
    private final ThreadedPrintStream err;
    private boolean started = false;

    /**
     * New COMPSs Master
     *
     * @param hostName
     */
    public COMPSsMaster(String hostName) {
        super(hostName, null);
        name = hostName;

        // Gets user execution directory
        userExecutionDirPath = System.getProperty("user.dir");

        /* Creates base Runtime structure directories ************************** */
        boolean mustCreateExecutionSandbox = true;
        // Checks if specific log base dir has been given
        String specificOpt = System.getProperty(COMPSsConstants.SPECIFIC_LOG_DIR);
        if (specificOpt != null && !specificOpt.isEmpty()) {
            COMPSsLogBaseDirPath = specificOpt.endsWith(File.separator) ? specificOpt : specificOpt + File.separator;
            mustCreateExecutionSandbox = false; // This is the only case where
            // the sandbox is provided
        } else {
            // Checks if base log dir has been given
            String baseOpt = System.getProperty(COMPSsConstants.BASE_LOG_DIR);
            if (baseOpt != null && !baseOpt.isEmpty()) {
                baseOpt = baseOpt.endsWith(File.separator) ? baseOpt : baseOpt + File.separator;
                COMPSsLogBaseDirPath = baseOpt + ".COMPSs" + File.separator;
            } else {
                // No option given - load default (user home)
                COMPSsLogBaseDirPath = System.getProperty("user.home") + File.separator + ".COMPSs" + File.separator;
            }
        }

        if (!new File(COMPSsLogBaseDirPath).exists()) {
            if (!new File(COMPSsLogBaseDirPath).mkdir()) {
                ErrorManager.error(ERROR_COMPSs_LOG_BASE_DIR + " at " + COMPSsLogBaseDirPath);
            }
        }

        // Load working directory. Different for regular applications and
        // services
        if (mustCreateExecutionSandbox) {
            String appName = System.getProperty(COMPSsConstants.APP_NAME);
            if (System.getProperty(COMPSsConstants.SERVICE_NAME) != null) {
                /*
                 * SERVICE - Gets appName - Overloads the service folder for different executions - MAX_OVERLOAD raises
                 * warning - Changes working directory to serviceName !!!!
                 */
                String serviceName = System.getProperty(COMPSsConstants.SERVICE_NAME);
                int overloadCode = 1;
                String appLog = COMPSsLogBaseDirPath + serviceName + "_0" + String.valueOf(overloadCode)
                        + File.separator;
                String oldest = appLog;
                while ((new File(appLog).exists()) && (overloadCode <= MAX_OVERLOAD)) {
                    // Check oldest file (for overload if needed)
                    if (new File(oldest).lastModified() > new File(appLog).lastModified()) {
                        oldest = appLog;
                    }
                    // Next step
                    overloadCode = overloadCode + 1;
                    if (overloadCode < 10) {
                        appLog = COMPSsLogBaseDirPath + serviceName + "_0" + String.valueOf(overloadCode)
                                + File.separator;
                    } else {
                        appLog = COMPSsLogBaseDirPath + serviceName + "_" + String.valueOf(overloadCode)
                                + File.separator;
                    }
                }
                if (overloadCode > MAX_OVERLOAD) {
                    // Select the last modified folder
                    appLog = oldest;

                    // Overload
                    System.err.println(WARN_FOLDER_OVERLOAD);
                    System.err.println("Overwriting entry: " + appLog);

                    // Clean previous results to avoid collisions
                    if (!deleteDirectory(new File(appLog))) {
                        ErrorManager.error(ERROR_APP_OVERLOAD);
                    }
                }

                // We have the final appLogDirPath
                appLogDirPath = appLog;
                if (!new File(appLogDirPath).mkdir()) {
                    ErrorManager.error(ERROR_APP_LOG_DIR);
                }
            } else {
                /*
                 * REGULAR APPLICATION - Gets appName - Overloads the app folder for different executions - MAX_OVERLOAD
                 * raises warning - Changes working directory to appName !!!!
                 */
                int overloadCode = 1;
                String appLog = COMPSsLogBaseDirPath + appName + "_0" + String.valueOf(overloadCode) + File.separator;
                String oldest = appLog;
                while ((new File(appLog).exists()) && (overloadCode <= MAX_OVERLOAD)) {
                    // Check oldest file (for overload if needed)
                    if (new File(oldest).lastModified() > new File(appLog).lastModified()) {
                        oldest = appLog;
                    }
                    // Next step
                    overloadCode = overloadCode + 1;
                    if (overloadCode < 10) {
                        appLog = COMPSsLogBaseDirPath + appName + "_0" + String.valueOf(overloadCode) + File.separator;
                    } else {
                        appLog = COMPSsLogBaseDirPath + appName + "_" + String.valueOf(overloadCode) + File.separator;
                    }
                }
                if (overloadCode > MAX_OVERLOAD) {
                    // Select the last modified folder
                    appLog = oldest;

                    // Overload
                    System.err.println(WARN_FOLDER_OVERLOAD);
                    System.err.println("Overwriting entry: " + appLog);

                    // Clean previous results to avoid collisions
                    if (!deleteDirectory(new File(appLog))) {
                        ErrorManager.error(ERROR_APP_OVERLOAD);
                    }
                }

                // We have the final appLogDirPath
                appLogDirPath = appLog;
                if (!new File(appLogDirPath).mkdir()) {
                    ErrorManager.error(ERROR_APP_LOG_DIR);
                }
            }
        } else {
            // The option specific_log_dir has been given. NO sandbox created
            appLogDirPath = COMPSsLogBaseDirPath;
        }

        // Set the environment property (for all cases) and reload logger
        // configuration
        System.setProperty(COMPSsConstants.APP_LOG_DIR, appLogDirPath);
        ((LoggerContext) LogManager.getContext(false)).reconfigure();

        /*
         * Create a tmp directory where to store: - Files whose first opened stream is an input one - Object files
         */
        tempDirPath = appLogDirPath + "tmpFiles" + File.separator;
        if (!new File(tempDirPath).mkdir()) {
            ErrorManager.error(ERROR_TEMP_DIR);
        }

        /*
         * Create a jobs dir where to store: - Jobs output files - Jobs error files
         */
        jobsDirPath = appLogDirPath + "jobs" + File.separator;
        if (!new File(jobsDirPath).mkdir()) {
            ErrorManager.error(ERROR_JOBS_DIR);
        }

        /*
         * Create a workers dir where to store: - Worker out files - Worker error files
         */
        workersDirPath = appLogDirPath + "workers" + File.separator;
        if (!new File(workersDirPath).mkdir()) {
            System.err.println(ERROR_WORKERS_DIR);
            System.exit(1);
        }

        // Configure worker debug level
        // Configure storage
        String storageConf = System.getProperty(COMPSsConstants.STORAGE_CONF);
        if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
            storageConf = "null";
            LOGGER.warn("No storage configuration file passed");
        }
        this.storageConf = storageConf;

        String executionType = System.getProperty(COMPSsConstants.TASK_EXECUTION);
        if (executionType == null || executionType.equals("") || executionType.equals("null")) {
            executionType = COMPSsConstants.TaskExecution.COMPSS.toString();
            LOGGER.warn("No executionType passed");
        } else {
            executionType = executionType.toUpperCase();
        }
        this.executionType = TaskExecution.valueOf(executionType);

        out = new ThreadedPrintStream(SUFFIX_OUT, System.out);
        err = new ThreadedPrintStream(SUFFIX_ERR, System.err);
        System.setErr(err);
        System.setOut(out);

        // Get installDir classpath
        this.installDirPath = System.getenv(COMPSsConstants.COMPSS_HOME);

        // Get worker classpath
        String classPath = System.getProperty(COMPSsConstants.WORKER_CP);
        if (classPath == null || classPath.isEmpty()) {
            classPath = "";
        }

        // Get appDir classpath
        String appDir = System.getProperty(COMPSsConstants.WORKER_APPDIR);
        if (appDir == null || appDir.isEmpty()) {
            appDir = "";
        }
        this.appDirPath = appDir;

        // Get python interpreter
        String pythonInterpreter = System.getProperty(COMPSsConstants.PYTHON_INTERPRETER);
        if (pythonInterpreter == null || pythonInterpreter.isEmpty() || pythonInterpreter.equals("null")) {
            pythonInterpreter = COMPSsConstants.DEFAULT_PYTHON_INTERPRETER;
        }

        // Get python version
        String pythonVersion = System.getProperty(COMPSsConstants.PYTHON_VERSION);
        if (pythonVersion == null || pythonVersion.isEmpty() || pythonVersion.equals("null")) {
            pythonVersion = COMPSsConstants.DEFAULT_PYTHON_VERSION;
        }

        // Configure python virtual environment
        String pythonVEnv = System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT);
        if (pythonVEnv == null || pythonVEnv.isEmpty() || pythonVEnv.equals("null")) {
            pythonVEnv = COMPSsConstants.DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT;
        }
        String pythonPropagateVEnv = System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
        if (pythonPropagateVEnv == null || pythonPropagateVEnv.isEmpty() || pythonPropagateVEnv.equals("null")) {
            pythonPropagateVEnv = COMPSsConstants.DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT;
        }

        String pythonPath = System.getProperty(COMPSsConstants.WORKER_PP);
        if (pythonPath == null || pythonPath.isEmpty()) {
            pythonPath = "";
        }

        // Get Python MPI worker invocation
        String pythonMpiWorker = System.getProperty(COMPSsConstants.PYTHON_MPI_WORKER);
        if (pythonMpiWorker == null || pythonMpiWorker.isEmpty() || pythonMpiWorker.equals("null")) {
            pythonMpiWorker = COMPSsConstants.DEFAULT_PYTHON_MPI_WORKER;
        }
        JavaParams javaParams = new JavaParams(classPath);
        PythonParams pyParams = new PythonParams(pythonInterpreter, pythonVersion, pythonVEnv, pythonPropagateVEnv,
                pythonPath, pythonMpiWorker);
        CParams cParams = new CParams(classPath);

        this.langParams[Lang.JAVA.ordinal()] = javaParams;
        this.langParams[Lang.PYTHON.ordinal()] = pyParams;
        this.langParams[Lang.C.ordinal()] = cParams;

        String workerPersistentC = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (workerPersistentC == null || workerPersistentC.isEmpty() || workerPersistentC.equals("null")) {
            workerPersistentC = COMPSsConstants.DEFAULT_PERSISTENT_C;
        }
        this.persistentEnabled = workerPersistentC.toUpperCase().compareTo("TRUE") == 0;

        this.executionManager = new ExecutionManager(this, 0, ThreadBinder.BINDER_DISABLED, 0,
                ThreadBinder.BINDER_DISABLED, 0, ThreadBinder.BINDER_DISABLED, 0);
        try {
            this.executionManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(EXECUTION_MANAGER_ERR, ie);
        }
    }

    private boolean deleteDirectory(File directory) {
        if (!directory.exists()) {
            return false;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    if (!f.delete()) {
                        return false;
                    }
                }
            }
        }

        return directory.delete();
    }

    @Override
    public void start() {
        synchronized (this) {
            if (started) {
                return;
            }
            started = true;
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setInternalURI(MultiURI u) {
        for (CommAdaptor adaptor : Comm.getAdaptors().values()) {
            adaptor.completeMasterURI(u);
        }
    }

    @Override
    public void stop(ShutdownListener sl) {
        // ExecutionManager was already shutdown
        sl.notifyEnd();
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
            Transferable reason, EventListener listener) {

        for (Resource targetRes : target.getHosts()) {
            COMPSsNode node = targetRes.getNode();
            if (node != this) {
                try {
                    node.obtainData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    // Can not copy the file.
                    // Cannot receive the file, try with the following
                    continue;
                }
                return;
            }

        }
    }

    public void obtainBindingData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
            Transferable reason, EventListener listener) {
        BindingObject tgtBO = ((BindingObjectLocation) target).getBindingObject();
        ld.lockHostRemoval();
        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            for (Copy copy : copiesInProgress) {
                if (copy != null) {
                    if (copy.getTargetLoc() != null && copy.getTargetLoc().getHosts().contains(Comm.getAppHost())) {
                        if (DEBUG) {
                            LOGGER.debug(
                                    "Copy in progress tranfering " + ld.getName() + "to master. Waiting for finishing");
                        }
                        Copy.waitForCopyTofinish(copy, this);
                        // try {
                        if (DEBUG) {
                            LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget() + " to "
                                    + tgtBO.getName());
                        }
                        try {
                            if (persistentEnabled) {
                                manageObtainBindingObjectInCache(copy.getFinalTarget(), tgtBO, tgtData, target, reason);
                            }else {
                                manageObtainBindingObjectAsFile(copy.getFinalTarget(), tgtBO, tgtData, target, reason);
                            }
                            listener.notifyEnd(null);
                        }catch(Exception e){
                            LOGGER.error("ERROR: managing obtain binding object at cache", e);
                            listener.notifyFailure(null, e);
                        }
                        ld.releaseHostRemoval();
                        return;

                    } else {
                        if (copy.getTargetData() != null
                                && copy.getTargetData().getAllHosts().contains(Comm.getAppHost())) {
                            Copy.waitForCopyTofinish(copy, this);
                            // try {
                            if (DEBUG) {
                                LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget() + " to "
                                        + tgtBO.getName());
                            }
                            try {
                                if (persistentEnabled) {
                                    manageObtainBindingObjectInCache(copy.getFinalTarget(), tgtBO, tgtData, target, reason);
                                }else {
                                    manageObtainBindingObjectAsFile(copy.getFinalTarget(), tgtBO, tgtData, target, reason);
                                }
                                listener.notifyEnd(null);
                            }catch(Exception e){
                                LOGGER.error("ERROR: managing obtain binding object at cache", e);
                                listener.notifyFailure(null, e);
                            }
                            ld.releaseHostRemoval();
                            return;

                        } else {
                            if (DEBUG) {
                                LOGGER.debug("Current copies are not transfering " + ld.getName()
                                        + " to master. Ignoring at this moment");
                            }
                        }
                    }
                }
            }
        }

        // Checking if file is already in master
        if (DEBUG) {
            LOGGER.debug("Checking if " + ld.getName() + " is at master (" + Comm.getAppHost().getName() + ").");
        }

        for (MultiURI u : ld.getURIs()) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(ld.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost() == Comm.getAppHost()) {
                if (DEBUG) {
                    LOGGER.debug("Master local copy " + ld.getName() + " from " + u.getHost().getName() + " to "
                            + tgtBO.getName());
                }
                try {
                    if(persistentEnabled) {
                        manageObtainBindingObjectInCache(u.getPath(), tgtBO, tgtData, target, reason);
                    }else {
                        manageObtainBindingObjectAsFile(u.getPath(), tgtBO, tgtData, target, reason);
                    }
                    listener.notifyEnd(null);
                }catch(Exception e){
                    LOGGER.error("ERROR: managing obtain binding object at cache", e);
                    listener.notifyFailure(null, e);
                }
                ld.releaseHostRemoval();
                return;
            } else {
                if (DEBUG) {
                    String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                    LOGGER.debug("Data " + ld.getName() + " copy in " + hostname + " not evaluated now");
                }
            }

        }

        // Ask the transfer from an specific source
        if (source != null) {
            for (Resource sourceRes : source.getHosts()) {
                COMPSsNode node = sourceRes.getNode();
                String sourcePath = source.getURIInHost(sourceRes).getPath();
                if (node != this) {
                    try {
                        if (DEBUG) {
                            LOGGER.debug("Sending data " + ld.getName() + " from (" + node.getName() + ") " + sourcePath
                                    + " to (master) " + tgtBO.getName());
                        }
                        node.sendData(ld, source, target, tgtData, reason, listener);
                    } catch (Exception e) {
                        ErrorManager.warn("Not possible to sending data master to " + tgtBO.getName(), e);
                        continue;
                    }
                    LOGGER.debug("Data " + ld.getName() + " sent.");
                    ld.releaseHostRemoval();
                    return;
                } else {
                    try {
                        if(persistentEnabled) {
                            manageObtainBindingObjectInCache(sourcePath, tgtBO, tgtData, target, reason);
                        }else {
                            manageObtainBindingObjectAsFile(sourcePath, tgtBO, tgtData, target, reason);
                        }
                        listener.notifyEnd(null);
                    }catch(Exception e){
                        LOGGER.error("ERROR: managing obtain binding object at cache", e);
                        listener.notifyFailure(null, e);
                    }
                    ld.releaseHostRemoval();
                    return;
                }
            }
        } else {
            LOGGER.debug("Source data location is null. Trying other alternatives");
        }

        // Preferred source is null or copy has failed. Trying to retrieve data from any host
        for (Resource sourceRes : ld.getAllHosts()) {
            COMPSsNode node = sourceRes.getNode();
            if (node != this) {
                try {
                    LOGGER.debug("Sending data " + ld.getName() + " from (" + node.getName() + ") "
                            + sourceRes.getName() + " to (master)" + tgtBO.getName());
                    node.sendData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    LOGGER.error("Error: exception sending data", e);
                    continue;
                }
                LOGGER.debug("Data " + ld.getName() + " sent.");
                ld.releaseHostRemoval();
                return;
            } else {
                if (DEBUG) {
                    LOGGER.debug("Data " + ld.getName() + " copy in " + sourceRes.getName()
                            + " not evaluated now. Should have been evaluated before");
                }
            }
        }
        LOGGER.warn("WARN: All posibilities checked for obtaining data " + ld.getName() + " and nothing done. Releasing listeners and locks");
        listener.notifyEnd(null);
        ld.releaseHostRemoval();
    }

    private void manageObtainBindingObjectInCache(String initialPath, BindingObject tgtBO, LogicalData tgtData,
            DataLocation target, Transferable reason) throws Exception {
        BindingObject bo = BindingObject.generate(initialPath);
        
        if (bo.getName().equals(tgtBO.getName())) {
            if (BindingDataManager.isInBinding(tgtBO.getName())) {
                LOGGER.debug(
                    "Current transfer is the same as expected. Nothing to do setting data target to "
                    + initialPath);
                reason.setDataTarget(initialPath);
            }else {
                String tgtPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
                LOGGER.debug("Data " + tgtBO.getName() + " not in cache loading from file " + tgtPath);
                if (BindingDataManager.loadFromFile(tgtBO.getName(), tgtPath, tgtBO.getType(), tgtBO.getElements())!=0) {
                    throw(new Exception("Error loading object " + tgtBO.getName() + " from " + tgtPath));
                }   
                reason.setDataTarget(target.getPath());
            }
        } else {
            if (BindingDataManager.isInBinding(tgtBO.getName())) {
                LOGGER.debug("Making cache copy from " + bo.getName() + " to " + tgtBO.getName());
                if (reason.isSourcePreserved()) {
                    if(BindingDataManager.copyCachedData(bo.getName(), tgtBO.getName())!=0){
                        throw(new Exception("Error copying cache from " + bo.getName() + " to "+ tgtBO.getName()));
                    }
                } else {
                    if(BindingDataManager.moveCachedData(bo.getName(), tgtBO.getName())!=0){
                        throw(new Exception("Error moved cache from " + bo.getName() + " to "+ tgtBO.getName()));
                    }
                }
            }else {
                String tgtPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
                LOGGER.debug("Data "+ tgtBO.getName()+" not in cache loading from file " + tgtPath);
                if (BindingDataManager.loadFromFile(tgtBO.getName(), tgtPath, tgtBO.getType(), tgtBO.getElements())!=0) {
                    throw(new Exception("Error loading object " + tgtBO.getName() + " from " + tgtPath));
                }
            }
            if (tgtData != null) {
                tgtData.addLocation(target);
            }
            LOGGER.debug("BindingObject copied/moved set data target as " + target.getPath());
            reason.setDataTarget(target.getPath());
        }
        
        
    }
    
    private void manageObtainBindingObjectAsFile(String initialPath, BindingObject tgtBO, LogicalData tgtData,
            DataLocation target, Transferable reason) throws Exception {
        BindingObject bo = BindingObject.generate(initialPath);
        if (bo.getName().equals(tgtBO.getName())) {
            LOGGER.debug(
                    "Current transfer is the same as expected. Nothing to do setting data target to "
                    + initialPath);
            reason.setDataTarget(initialPath);
        }else {
            if(bo.getId().startsWith(File.separator)) {
                String iPath = getCompletePath(DataType.BINDING_OBJECT_T, bo.getName()).getPath();
                String tPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
                if (reason.isSourcePreserved()) {
                        if (DEBUG) {
                            LOGGER.debug("Master local copy of data" + bo.getName() + " from " + iPath + " to "
                                + tPath);
                        }
                        Files.copy(new File(iPath).toPath(), new File(tPath).toPath(),
                            StandardCopyOption.REPLACE_EXISTING);

                } else {
                    if (DEBUG) {
                        LOGGER.debug("Master local move of data " + bo.getName() + " from " + iPath + " to "
                        + tPath);
                    }
                    Files.move(new File(iPath).toPath(), new File(tPath).toPath(),
                            StandardCopyOption.REPLACE_EXISTING);
                }
            } else if (BindingDataManager.isInBinding(bo.getName())) {
                String tPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
                LOGGER.debug("Storing object data " + bo.getName() + " from cache to " + tPath);
                BindingDataManager.storeInFile(bo.getName(), tPath);
            } else {
                throw new Exception("Data " + bo.getName() + "not a filepath and its not in cache");
            }

            if (tgtData != null) {
                tgtData.addLocation(target);
            }
            
            LOGGER.debug("BindingObject as file copied/moved set data target as " + target.getPath());
            reason.setDataTarget(target.getPath());
        }
        // If path is relative push to cache. If not keep as file (master_in_worker)
        LOGGER.debug(" Checking if BindingObject " + tgtBO.getId() + " has relative path");
        if (!tgtBO.getId().startsWith(File.separator)) {
            LOGGER.debug("Loading BindingObject " + tgtBO.getName() + " to cache...");
            String tgtPath = getCompletePath(DataType.BINDING_OBJECT_T, tgtBO.getName()).getPath();
            if (BindingDataManager.loadFromFile(tgtBO.getName(), tgtPath, tgtBO.getType(), tgtBO.getElements()) != 0) {
                throw (new Exception("Error loading object " + tgtBO.getName() + " from " + tgtPath));
            }
        }
    }

    public void obtainFileData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
            Transferable reason, EventListener listener) {

        String targetPath = target.getURIInHost(Comm.getAppHost()).getPath();

        //Check if file is already on the Path
        List<MultiURI> uris = ld.getURIs();
        for (MultiURI u : uris) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(ld.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost().getNode() == this) {
                if (targetPath.compareTo(u.getPath()) == 0) {
                    LOGGER.debug(ld.getName() + " is already at " + targetPath);
                    // File already in the Path
                    reason.setDataTarget(targetPath);
                    listener.notifyEnd(null);
                    return;
                }
            }
        }

        // Check if there are current copies in progress bringing it into the node.
        if (DEBUG) {
            LOGGER.debug(
                    "Data " + ld.getName() + " not in memory. Checking if there is a copy to the master in progress");
        }
        ld.lockHostRemoval();
        Collection<Copy> copiesInProgress = ld.getCopiesInProgress();
        if (copiesInProgress != null && !copiesInProgress.isEmpty()) {
            for (Copy copy : copiesInProgress) {
                if (copy != null) {
                    if (copy.getTargetLoc() != null && copy.getTargetLoc().getHosts().contains(Comm.getAppHost())) {
                        if (DEBUG) {
                            LOGGER.debug(
                                    "Copy in progress tranfering " + ld.getName() + "to master. Waiting for finishing");
                        }
                        Copy.waitForCopyTofinish(copy, this);
                        try {
                            if (DEBUG) {
                                LOGGER.debug("Master local copy " + ld.getName() + " from " + copy.getFinalTarget()
                                        + " to " + targetPath);
                            }
                            Files.copy((new File(copy.getFinalTarget())).toPath(), new File(targetPath).toPath(),
                                    StandardCopyOption.REPLACE_EXISTING);
                            if (tgtData != null) {
                                tgtData.addLocation(target);
                            }
                            LOGGER.debug("File copied set dataTarget " + targetPath);
                            reason.setDataTarget(targetPath);

                            listener.notifyEnd(null);
                            ld.releaseHostRemoval();
                            return;
                        } catch (IOException ex) {
                            ErrorManager.warn("Error master local copying file " + copy.getFinalTarget()
                                    + " from master to " + targetPath + " with replacing", ex);
                        }

                    }
                }
            }
        }

        // Checking if file is already in master
        if (DEBUG) {
            LOGGER.debug("Checking if " + ld.getName() + " is at master (" + Comm.getAppHost().getName() + ").");
        }

        for (MultiURI u : uris) {
            if (DEBUG) {
                String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                LOGGER.debug(ld.getName() + " is at " + u.toString() + "(" + hostname + ")");
            }
            if (u.getHost().getNode() == this) {
                try {
                    if (DEBUG) {
                        LOGGER.debug("Data " + ld.getName() + " is already accessible at " + u.getPath());
                    }
                    if (reason.isSourcePreserved()) {
                        if (DEBUG) {
                            LOGGER.debug("Master local copy " + ld.getName() + " from " + u.getHost().getName() + " to "
                                    + targetPath);
                        }
                        Files.copy(
                                (new File(u.getPath())).toPath(),
                                new File(targetPath).toPath(),
                                StandardCopyOption.REPLACE_EXISTING);

                    } else {
                        if (DEBUG) {
                            LOGGER.debug("Master local copy " + ld.getName() + " from " + u.getHost().getName() + " to "
                                    + targetPath);
                        }
                        Files.move(
                                (new File(u.getPath())).toPath(),
                                new File(targetPath).toPath(),
                                StandardCopyOption.REPLACE_EXISTING);
                        uris.remove(u);
                    }

                    if (tgtData != null) {
                        tgtData.addLocation(target);
                    }
                    LOGGER.debug("File on path. Set data target to " + targetPath);
                    reason.setDataTarget(targetPath);

                    listener.notifyEnd(null);
                    ld.releaseHostRemoval();
                    return;
                } catch (IOException ex) {
                    ErrorManager.warn("Error master local copy file from " + u.getPath() + " to " + targetPath
                            + " with replacing", ex);
                }
            } else {
                if (DEBUG) {
                    String hostname = (u.getHost() != null) ? u.getHost().getName() : "null";
                    LOGGER.debug("Data " + ld.getName() + " copy in " + hostname + " not evaluated now");
                }
            }
        }

        // Ask the transfer from an specific source
        if (source != null) {
            for (Resource sourceRes : source.getHosts()) {
                COMPSsNode node = sourceRes.getNode();
                String sourcePath = source.getURIInHost(sourceRes).getPath();
                if (node != this) {
                    try {
                        if (DEBUG) {
                            LOGGER.debug("Sending data " + ld.getName() + " from " + sourcePath + " to " + targetPath);
                        }
                        node.sendData(ld, source, target, tgtData, reason, listener);
                    } catch (Exception e) {
                        ErrorManager.warn("Not possible to sending data master to " + targetPath, e);
                        continue;
                    }
                    LOGGER.debug("Data " + ld.getName() + " sent.");
                    ld.releaseHostRemoval();
                    return;
                } else {
                    try {
                        if (DEBUG) {
                            LOGGER.debug("Local copy " + ld.getName() + " from " + sourcePath + " to " + targetPath);
                        }
                        Files.copy(new File(sourcePath).toPath(), new File(targetPath).toPath(),
                                StandardCopyOption.REPLACE_EXISTING);

                        LOGGER.debug("File copied. Set data target to " + targetPath);
                        reason.setDataTarget(targetPath);
                        listener.notifyEnd(null);
                        ld.releaseHostRemoval();
                        return;
                    } catch (IOException ex) {
                        ErrorManager.warn("Error master local copy file from " + sourcePath + " to " + targetPath, ex);
                    }
                }
            }
        } else {
            LOGGER.debug("Source data location is null. Trying other alternatives");
        }

        // Preferred source is null or copy has failed. Trying to retrieve data from any host
        for (Resource sourceRes : ld.getAllHosts()) {
            COMPSsNode node = sourceRes.getNode();
            if (node != this) {
                try {
                    LOGGER.debug("Sending data " + ld.getName() + " from " + sourceRes.getName() + " to " + targetPath);
                    node.sendData(ld, source, target, tgtData, reason, listener);
                } catch (Exception e) {
                    LOGGER.error("Error: exception sending data", e);
                    continue;
                }
                LOGGER.debug("Data " + ld.getName() + " sent.");
                ld.releaseHostRemoval();
                return;
            } else {
                if (DEBUG) {
                    LOGGER.debug("Data " + ld.getName() + " copy in " + sourceRes.getName()
                            + " not evaluated now. Should have been evaluated before");
                }
            }
        }

        // If we have not exited before, any copy method was successful. Raise warning
        ErrorManager.warn("Error file " + ld.getName() + " not transferred to " + targetPath);
        listener.notifyEnd(null);
        ld.releaseHostRemoval();
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
            Transferable reason, EventListener listener) {
        LOGGER.info("Obtain Data " + ld.getName());
        if (DEBUG) {
            if (ld != null) {
                LOGGER.debug("srcData: " + ld.toString());
            }
            if (reason != null) {
                LOGGER.debug("Reason: " + reason.getType());
            }
            if (source != null) {
                LOGGER.debug("Source Data location: " + source.getType().toString() + " "
                        + source.getProtocol().toString() + " " + source.getURIs().get(0));
            }
            if (target != null) {
                if (target.getProtocol() != Protocol.PERSISTENT_URI) {
                    LOGGER.debug("Target Data location: " + target.getType().toString() + " "
                            + target.getProtocol().toString() + " " + target.getURIs().get(0));
                } else {
                    LOGGER.debug("Target Data location: " + target.getType().toString() + " "
                            + target.getProtocol().toString());
                }
            }
            if (tgtData != null) {
                LOGGER.debug("tgtData: " + tgtData.toString());
            }
        }
        /*
         * Check if data is binding data
         */
        if (ld.isBindingData() || (reason != null && reason.getType().equals(DataType.BINDING_OBJECT_T))
                || (source != null && source.getType().equals(Type.BINDING))
                || (target != null && target.getType().equals(Type.BINDING))) {
            obtainBindingData(ld, source, target, tgtData, reason, listener);
            return;
        }
        /*
         * PSCO transfers are always available, if any SourceLocation is PSCO, don't transfer
         */

        for (DataLocation loc : ld.getLocations()) {
            if (loc.getProtocol().equals(Protocol.PERSISTENT_URI)) {
                LOGGER.debug("Object in Persistent Storage. Set dataTarget to " + loc.getPath());
                reason.setDataTarget(loc.getPath());
                listener.notifyEnd(null);
                return;
            }
        }

        /*
         * Otherwise the data is a file or an object that can be already in the master memory, in the master disk or
         * being transfered
         */
        // Check if data is in memory (no need to check if it is PSCO since previous case avoids it)
        if (ld.isInMemory()) {
            String targetPath = target.getURIInHost(Comm.getAppHost()).getPath();
            // Serialize value to file
            try {
                Serializer.serialize(ld.getValue(), targetPath);
            } catch (IOException ex) {
                ErrorManager.warn("Error copying file from memory to " + targetPath, ex);
            }

            if (tgtData != null) {
                tgtData.addLocation(target);
            }
            LOGGER.debug("Object in memory. Set dataTarget to " + targetPath);
            reason.setDataTarget(targetPath);
            listener.notifyEnd(null);
            return;
        }

        obtainFileData(ld, source, target, tgtData, reason, listener);

    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
        //Copy already done on obtainData()
        listener.notifyEnd(null);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
            List<String> slaveWorkersNodeNames, JobListener listener) {
        return new LocalJob(taskId, taskParams, impl, res, slaveWorkersNodeNames, listener);
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        String path = null;
        switch (type) {
            case FILE_T:
                path = Protocol.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + name;
                break;
            case OBJECT_T:
            case COLLECTION_T:
                path = Protocol.OBJECT_URI.getSchema() + name;
                break;
            case PSCO_T:
                path = Protocol.PERSISTENT_URI.getSchema() + name;
                break;
            case EXTERNAL_PSCO_T:
                path = Protocol.PERSISTENT_URI.getSchema() + name;
                break;
            case BINDING_OBJECT_T:
                path = Protocol.BINDING_URI.getSchema() + Comm.getAppHost().getTempDirPath() + name;
                break;
            default:
                return null;
        }

        // Switch path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
        File dir = new File(Comm.getAppHost().getTempDirPath());
        for (File f : dir.listFiles()) {
            deleteFolder(f);
        }
    }

    private void deleteFolder(File folder) {
        if (folder.isDirectory()) {
            for (File f : folder.listFiles()) {
                deleteFolder(f);
            }
        }
        if (!folder.delete()) {
            LOGGER.error("Error deleting file " + (folder == null ? "" : folder.getName()));
        }
    }

    @Override
    public boolean generatePackage() {
        // Should not be executed on a COMPSsMaster
        return false;
    }

    @Override
    public boolean generateWorkersDebugInfo() {
        // Should not be executed on a COMPSsMaster
        return false;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        // Should not be executed on a COMPSsMaster
        this.executionManager.stop();
        sl.notifyEnd();
    }

    @Override
    public String getUser() {
        return "";
    }

    @Override
    public String getClasspath() {
        return "";
    }

    @Override
    public String getPythonpath() {
        return "";
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void announceDestruction() throws AnnounceException {
        // No need to do it. The master no it's always up
    }

    @Override
    public void announceCreation() throws AnnounceException {
        // No need to do it. The master no it's always up
    }

    public void runJob(LocalJob job) {
        Execution exec = new Execution(job, new ExecutionListener() {

            @Override
            public void notifyEnd(Invocation invocation, boolean success) {
                if (success) {
                    job.getListener().jobCompleted(job);
                } else {
                    job.getListener().jobFailed(job, JobListener.JobEndStatus.EXECUTION_FAILED);
                }
            }
        });
        executionManager.enqueue(exec);
    }

    @Override
    public String getHostName() {
        return this.name;
    }

    @Override
    public long getTracingHostID() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getAppDir() {
        return this.appDirPath;
    }

    @Override
    public String getInstallDir() {
        return this.installDirPath;
    }

    @Override
    public String getWorkingDir() {
        return this.tempDirPath;
    }

    @Override
    public COMPSsConstants.TaskExecution getExecutionType() {
        return this.executionType;
    }

    @Override
    public boolean isPersistentCEnabled() {
        return this.persistentEnabled;
    }

    @Override
    public LanguageParams getLanguageParams(COMPSsConstants.Lang language) {
        return this.langParams[language.ordinal()];
    }

    @Override
    public void registerOutputs(String path) {
        err.registerThread(path);
        out.registerThread(path);
    }

    @Override
    public void unregisterOutputs() {
        err.unregisterThread();
        out.unregisterThread();
    }

    @Override
    public String getStandardStreamsPath(Invocation invocation) {
        // Set outputs paths (Java will register them, ExternalExec will redirect processes outputs)
        return Comm.getAppHost().getJobsDirPath() + File.separator + "job" + invocation.getJobId() + "_"
                + invocation.getHistory();
    }

    @Override
    public PrintStream getThreadOutStream() {
        return out.getStream();
    }

    @Override
    public PrintStream getThreadErrStream() {
        return err.getStream();
    }

    @Override
    public String getStorageConf() {
        return this.storageConf;
    }

    @Override
    public void loadParam(InvocationParam invParam) throws Exception {
        LocalParameter localParam = (LocalParameter) invParam;

        switch (localParam.getType()) {
            case FILE_T:
                // No need to load anything. Value already on a file
                break;
            case OBJECT_T: {
                DependencyParameter dpar = (DependencyParameter) localParam.getParam();
                String dataId = (String) localParam.getValue();
                LogicalData ld = Comm.getData(dataId);
                if (ld.isInMemory()) {
                    invParam.setValue(ld.getValue());
                } else {
                    Object o = Serializer.deserialize(dpar.getDataTarget());
                    invParam.setValue(o);
                }
                break;
            }
            case PSCO_T: {
                String pscoId = (String) localParam.getValue();
                Object o = StorageItf.getByID(pscoId);
                invParam.setValue(o);
                break;
            }
            default:
            // Already contains the proper value on the param
        }
    }

    @Override
    public void storeParam(InvocationParam invParam) throws Exception {
        LocalParameter localParam = (LocalParameter) invParam;
        Parameter param = localParam.getParam();
        switch (param.getType()) {
            case FILE_T:
                // No need to store anything. Already stored on disk
                break;
            case OBJECT_T: {
                String resultName = localParam.getDataMgmtId();
                LogicalData ld = Comm.getData(resultName);
                ld.setValue(invParam.getValue());
                break;
            }
            case BINDING_OBJECT_T:
                //No need to store anything. Already stored on the binding
                break;
            default:
                throw new UnsupportedOperationException("Not supported yet." + param.getType());
        }
    }

    public String getCOMPSsLogBaseDirPath() {
        return this.COMPSsLogBaseDirPath;
    }

    public String getWorkingDirectory() {
        return this.tempDirPath;
    }

    public String getUserExecutionDirPath() {
        return this.userExecutionDirPath;
    }

    public String getAppLogDirPath() {
        return this.appLogDirPath;
    }

    public String getTempDirPath() {
        return this.tempDirPath;
    }

    public String getJobsDirPath() {
        return this.jobsDirPath;
    }

    public String getWorkersDirPath() {
        return this.workersDirPath;
    }

    @Override
    public void increaseComputingCapabilities(ResourceDescription descr) {
        MethodResourceDescription description = (MethodResourceDescription) descr;
        int cpuCount = description.getTotalCPUComputingUnits();
        int GPUCount = description.getTotalGPUComputingUnits();
        int FPGACount = description.getTotalFPGAComputingUnits();
        int otherCount = description.getTotalOTHERComputingUnits();
        executionManager.increaseCapabilities(cpuCount, GPUCount, FPGACount, otherCount);
    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription descr) {
        MethodResourceDescription description = (MethodResourceDescription) descr;
        int cpuCount = description.getTotalCPUComputingUnits();
        int GPUCount = description.getTotalGPUComputingUnits();
        int FPGACount = description.getTotalFPGAComputingUnits();
        int otherCount = description.getTotalOTHERComputingUnits();
        executionManager.reduceCapabilities(cpuCount, GPUCount, FPGACount, otherCount);
    }
}
