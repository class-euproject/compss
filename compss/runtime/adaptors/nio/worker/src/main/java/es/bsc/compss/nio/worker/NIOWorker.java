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
package es.bsc.compss.nio.worker;

import es.bsc.comm.Connection;
import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsConstants.TaskExecution;
import es.bsc.compss.data.DataManager;
import es.bsc.compss.data.DataProvider;
import es.bsc.compss.data.FetchDataListener;
import es.bsc.compss.data.MultiOperationFetchListener;
import es.bsc.compss.executor.ExecutionManager;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.types.ExecutionListener;
import es.bsc.compss.executor.utils.ThreadedPrintStream;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.invokers.types.JavaParams;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOMessageHandler;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.commands.CommandDataReceived;
import es.bsc.compss.nio.commands.CommandExecutorShutdownACK;
import es.bsc.compss.nio.commands.CommandNIOTaskDone;
import es.bsc.compss.nio.commands.CommandShutdownACK;
import es.bsc.compss.nio.commands.NIOData;
import es.bsc.compss.nio.commands.workerFiles.CommandWorkerDebugFilesDone;
import es.bsc.compss.nio.dataRequest.DataRequest;
import es.bsc.compss.nio.datarequest.WorkerDataRequest;
import es.bsc.compss.nio.exceptions.DataNotAvailableException;
import es.bsc.compss.nio.worker.components.DataManagerImpl;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NIOWorker extends NIOAgent implements InvocationContext, DataProvider {

    // Logger
    private static final Logger WORKER_LOGGER = LogManager.getLogger(Loggers.WORKER);
    private static final boolean WORKER_LOGGER_DEBUG = WORKER_LOGGER.isDebugEnabled();

    // Error messages
    private static final String EXECUTION_MANAGER_ERR = "Error starting ExecutionManager";
    private static final String DATA_MANAGER_ERROR = "Error starting DataManager";
    private static final String ERROR_INCORRECT_NUM_PARAMS = "Error: Incorrect number of parameters";

    // JVM Flag for WorkingDir removal
    private static final boolean REMOVE_WD;

    // Processes to capture out/err of each job
    private static final ThreadedPrintStream OUT;
    private static final ThreadedPrintStream ERR;
    public static final String SUFFIX_OUT = ".out";
    public static final String SUFFIX_ERR = ".err";

    // Application dependent attributes
    private final String deploymentId;
    private final boolean transferLogs;

    private final String hostName;
    private final String workingDir;
    private final String installDir;
    private final String appDir;

    private final TaskExecution executionType;
    private final boolean persistentC;

    private final LanguageParams[] langParams;

    private final Map<Integer, Long> times;

    // Internal components
    private final ExecutionManager executionManager;
    private final DataManager dataManager;

    static {
        // Set REMOVE_WD flag
        String removeWDFlag = System.getProperty(COMPSsConstants.WORKER_REMOVE_WD);
        boolean removeWDFlagDefined = removeWDFlag != null && !removeWDFlag.isEmpty();
        REMOVE_WD = removeWDFlagDefined ? Boolean.valueOf(removeWDFlag) : true;

        // Set processes to capturer out/error
        OUT = new ThreadedPrintStream(SUFFIX_OUT, System.out);
        ERR = new ThreadedPrintStream(SUFFIX_ERR, System.err);
        System.setErr(ERR);
        System.setOut(OUT);
    }


    /**
     * Creates a new NIOWorker instance.
     * 
     * @param transferLogs Whether to transfer the output/error files or not.
     * @param snd Number of senders.
     * @param rcv Number of receivers.
     * @param hostName Worker hostname.
     * @param masterName Master hostname.
     * @param masterPort Master port.
     * @param streamingPort Streaming port.
     * @param computingUnitsCPU Worker CPU computing units.
     * @param computingUnitsGPU Worker GPU computing units.
     * @param computingUnitsFPGA Worker FPGA computing units
     * @param cpuMap String describing the thread-cpu mapping.
     * @param gpuMap String describing the thread-gpu mapping.
     * @param fpgaMap String describing the thread-fpga mapping.
     * @param limitOfTasks Limit of simultaneous tasks.
     * @param appUuid Application UUID.
     * @param traceFlag Tracing flag.
     * @param traceHost Tracing host name.
     * @param storageConf Storage configuration file path.
     * @param executionType Task execution type.
     * @param persistentC Whether to spawn persistent C workers or not.
     * @param workingDir Worker working directory.
     * @param installDir Installation directory.
     * @param appDir Application directory.
     * @param javaParams Java specific parameters.
     * @param pyParams Python specific parameters.
     * @param cParams C specific parameters.
     */
    public NIOWorker(boolean transferLogs, int snd, int rcv, String hostName, String masterName, int masterPort,
            int streamingPort, int computingUnitsCPU, int computingUnitsGPU, int computingUnitsFPGA, String cpuMap,
            String gpuMap, String fpgaMap, int limitOfTasks, String appUuid, String traceFlag, String traceHost,
            String storageConf, TaskExecution executionType, boolean persistentC, String workingDir, String installDir,
            String appDir, JavaParams javaParams, PythonParams pyParams, CParams cParams) {

        super(snd, rcv, masterPort);

        this.transferLogs = transferLogs;
        // Log worker creation
        WORKER_LOGGER.info("NIO Worker init");

        // Set tracing attributes and initialize module if needed
        this.tracingLevel = Integer.parseInt(traceFlag);
        NIOTracer.init(this.tracingLevel);
        if (NIOTracer.extraeEnabled()) {
            NIOTracer.emitEvent(NIOTracer.Event.START.getId(), NIOTracer.Event.START.getType());

            if (NIOTracer.extraeEnabled() || NIOTracer.scorepEnabled() || NIOTracer.mapEnabled()) {
                try {
                    this.tracingId = Integer.parseInt(traceHost);
                    NIOTracer.setWorkerInfo(installDir, hostName, workingDir, this.tracingId);
                } catch (Exception e) {
                    WORKER_LOGGER.error("No valid hostID provided to the tracing system. Provided ID: " + hostName);
                }
            }
        }

        // Set attributes
        this.deploymentId = appUuid;
        this.hostName = hostName;
        this.workingDir = (workingDir.endsWith(File.separator) ? workingDir : workingDir + File.separator);
        this.installDir = (installDir.endsWith(File.separator) ? installDir : installDir + File.separator);
        this.appDir = appDir.equals("null") ? "" : appDir;

        this.executionType = executionType;
        System.setProperty(COMPSsConstants.STORAGE_CONF, storageConf);
        this.persistentC = persistentC;

        this.langParams = new LanguageParams[Lang.values().length];
        this.langParams[Lang.JAVA.ordinal()] = javaParams;
        this.langParams[Lang.PYTHON.ordinal()] = pyParams;
        this.langParams[Lang.C.ordinal()] = cParams;

        this.times = new HashMap<>();

        // Set master node to null (will be set afterwards to the right value)
        this.masterNode = null;
        // If master name is defined set masterNode with the defined value
        if (masterName != null && !masterName.isEmpty() && !masterName.equals("null")) {
            this.masterNode = new NIONode(masterName, masterPort);
        }

        // Start DataManagerImpl
        this.dataManager = new DataManagerImpl(this.hostName, masterName, streamingPort, workingDir, this);

        try {
            this.dataManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(DATA_MANAGER_ERROR, ie);
        }

        this.executionManager = new ExecutionManager(this, computingUnitsCPU, cpuMap, computingUnitsGPU, gpuMap,
                computingUnitsFPGA, fpgaMap, limitOfTasks);

        if (tracingLevel == Tracer.BASIC_MODE) {
            Tracer.enablePThreads();
        }

        try {
            this.executionManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(EXECUTION_MANAGER_ERR, ie);
        }

        if (tracingLevel == Tracer.BASIC_MODE) {
            Tracer.disablePThreads();
        }
    }

    @Override
    public void setWorkerIsReady(String nodeName) {
        // Implemented on NIOAdaptor to notify that the worker is up and ready
    }

    @Override
    public void setMaster(NIONode master) {
        if (masterNode == null) {
            masterNode = new NIONode(master.getIp(), masterPort);
        }
    }

    @Override
    public boolean isMyUuid(String uuid, String nodeName) {
        return uuid.equals(this.deploymentId) && nodeName.equals(this.hostName);
    }

    @Override
    public void receivedNewTask(NIONode master, NIOTask task, List<String> obsoleteFiles) {
        WORKER_LOGGER.info("Received Job " + task);
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("ARGUMENTS:");
            for (InvocationParam param : task.getParams()) {
                WORKER_LOGGER.info("    -" + param.getPrefix() + " " + param.getType() + ":" + param.getValue());
            }
            WORKER_LOGGER.debug("TARGET:");
            if (task.getTarget() != null) {
                WORKER_LOGGER.info("    -" + task.getTarget().getPrefix() + " " + task.getTarget().getType() + ":"
                        + task.getTarget().getValue());
            }
            WORKER_LOGGER.debug("RESULTS:");
            for (InvocationParam param : task.getResults()) {
                WORKER_LOGGER.info("    -" + param.getPrefix() + " " + param.getType() + ":" + param.getValue());
            }
        }

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.Event.WORKER_RECEIVED_NEW_TASK.getId(),
                    Tracer.Event.WORKER_RECEIVED_NEW_TASK.getType());
        }
        final long obsolSt = System.currentTimeMillis();
        // Remove obsolete
        if (obsoleteFiles != null) {
            removeObsolete(obsoleteFiles);
        }
        final long obsolEnd = System.currentTimeMillis();
        final long obsolDuration = obsolEnd - obsolSt;

        // Demand files
        WORKER_LOGGER.info("Checking parameters");
        TaskFetchOperationsListener listener = new TaskFetchOperationsListener(task);
        int paramIdx = 0;
        for (NIOParam param : task.getParams()) {
            WORKER_LOGGER.info("Checking parameter " + param);
            paramIdx++;
            if (param.getData() != null) {
                // Parameter has associated data
                if (WORKER_LOGGER_DEBUG) {
                    WORKER_LOGGER.debug(
                            "- Checking transfers for data " + param.getDataMgmtId() + " for parameter " + paramIdx);
                }
                listener.addOperation();
                dataManager.fetchParam(param, paramIdx, listener);
            }
        }
        WORKER_LOGGER.info("Checking target");
        NIOParam targetParam = task.getTarget();
        if (targetParam != null) {
            // Parameter has associated data
            WORKER_LOGGER
                    .debug("- Checking transfers for data " + targetParam.getDataMgmtId() + " for target parameter");
            listener.addOperation();
            dataManager.fetchParam(targetParam, -1, listener);
        }

        // Request the transfers
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(listener.getTask().getTaskId(), Tracer.getTaskTransfersType());
        }
        requestTransfers();
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getTaskTransfersType());
        }
        final long paramsEnd = System.currentTimeMillis();
        final long paramsDuration = paramsEnd - obsolEnd;
        WORKER_LOGGER.info("[Profile] Obsolete Processing: " + obsolDuration + " Processing " + paramsDuration);
        WORKER_LOGGER.info("[Profile] Pending parameters: " + listener.getMissingOperations());
        times.put(task.getJobId(), paramsEnd);
        listener.enable();

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.WORKER_RECEIVED_NEW_TASK.getType());
        }
    }

    @Override
    public void receivedNewDataFetchOrder(NIOParam data, int transferId) {
        FetchDataOperationListener listener = new FetchDataOperationListener(transferId);

        if (data != null) {
            // Parameter has associated data
            WORKER_LOGGER.debug("- Checking transfers for data " + data.getDataMgmtId());
            listener.addOperation();
            dataManager.fetchParam(data, -1, listener);
        }

        // Request the transfers
        /*
         * if (Tracer.extraeEnabled()) { Tracer.emitEvent(listener.getTask().getTaskId(),
         * Tracer.getTaskTransfersType()); }
         */
        requestTransfers();
        /*
         * if (Tracer.extraeEnabled()) { Tracer.emitEvent(Tracer.EVENT_END, Tracer.getTaskTransfersType()); }
         */

    }

    @Override
    public void askForTransfer(InvocationParam param, int index, FetchDataListener listener) {
        DataRequest dr = new WorkerDataRequest(listener, param.getType(), ((NIOParam) param).getData(),
                (String) param.getValue());
        addTransferRequest(dr);
    }

    @Override
    protected void handleDataToSendNotAvailable(Connection c, NIOData d) {
        // Now only manage at C (python could do the same when cache available)
        WORKER_LOGGER.debug("Handling data not available");
        /*
         * if (Lang.valueOf(lang.toUpperCase()) == Lang.C) { String path = d.getFirstURI().getPath();
         * WORKER_LOGGER.debug("about to serialize"); if (executionManager.serializeExternalData(d.getDataMgmtId(),
         * path)) { c.sendDataFile(path); return; } }
         */
        // If error or not external
        ErrorManager.warn(
                "Data " + d.getDataMgmtId() + "in this worker " + this.getHostName() + " could not be sent to master.");
        c.finishConnection();
    }

    // This is called when the master couldn't send a data to the worker.
    // The master abruptly finishes the connection. The NIOMessageHandler
    // handles this as an error, which treats with its function handleError,
    // and notifies the worker in this case.
    @Override
    public void handleRequestedDataNotAvailableError(List<DataRequest> failedRequests, String dataId) {
        for (DataRequest dr : failedRequests) { // For every task pending on this request, flag it as an error
            WorkerDataRequest wdr = (WorkerDataRequest) dr;
            // Mark as an error task. When all the params've been consumed, sendTaskDone unsuccessful
            wdr.getListener().errorFetchingValue(dataId, new DataNotAvailableException(dataId));

        }
    }

    @Override
    public void receivedValue(Destination type, String dataId, Object object, List<DataRequest> achievedRequests) {
        if (type == Transfer.Destination.OBJECT) {
            WORKER_LOGGER.info("Received data " + dataId + " with associated object " + object);
            dataManager.storeValue(dataId, object);
        } else {
            WORKER_LOGGER.info("Received data " + dataId);
            dataManager.storeFile(dataId, (String) object);
        }
        for (DataRequest dr : achievedRequests) {
            WorkerDataRequest wdr = (WorkerDataRequest) dr;
            wdr.getListener().fetchedValue(dataId);
            if (NIOTracer.extraeEnabled()) {
                NIOTracer.emitDataTransferEvent(NIOTracer.TRANSFER_END);
            }
            if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug(
                    "Pending parameters: " + ((MultiOperationFetchListener) wdr.getListener()).getMissingOperations());
            }
        }
    }

    private void sendTaskDone(Invocation invocation, boolean successful) {
        NIOTask nt = (NIOTask) invocation;
        int jobId = nt.getJobId();
        int taskId = nt.getTaskId();
        // Notify task done
        Connection c = TM.startConnection(masterNode);

        NIOTaskResult tr = new NIOTaskResult(jobId, nt.getParams(), nt.getTarget(), nt.getResults());
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("RESULT FOR JOB " + jobId + " (TASK ID: " + taskId + ")");
            WORKER_LOGGER.debug(tr);
        }
        CommandNIOTaskDone cmd = new CommandNIOTaskDone(this, tr, successful);
        c.sendCommand(cmd);

        if (transferLogs || !successful) {
            // Check that output files already exists. If not exists generate an empty one.
            String taskFileOutName = this.getStandardStreamsPath(invocation) + ".out";
            checkStreamFileExistence(taskFileOutName, "out",
                    "Autogenerated Empty file. An error was produced before generating any log in the stdout");
            String taskFileErrName = this.getStandardStreamsPath(invocation) + ".err";
            checkStreamFileExistence(taskFileErrName, "err",
                    "Autogenerated Empty file. An error was produced before generating any log in the stderr");
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("Sending file " + taskFileOutName + ", for connection: " + c.hashCode());
            }
            c.sendDataFile(taskFileOutName);
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("Sending file " + taskFileErrName + ", for connection: " + c.hashCode());
            }
            c.sendDataFile(taskFileErrName);
        }

        c.finishConnection();

        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("Job " + jobId + "(Task " + taskId + ") send job done");
        }
    }

    private void checkStreamFileExistence(String taskFileName, String streamName, String errorMessage) {
        File taskFile = new File(taskFileName);
        if (!taskFile.exists()) {
            try (FileOutputStream stream = new FileOutputStream(taskFile)) {
                stream.write(errorMessage.getBytes());
                stream.close();
            } catch (IOException ioe) {
                WORKER_LOGGER.error("IOException writing worker " + streamName + " file: " + taskFile, ioe);
            }
        }
    }

    // Check if this task is ready to execute
    private void executeTask(NIOTask task) {
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("Enqueueing job " + task.getJobId() + " for execution.");
        }

        // Execute the job
        Execution e = new Execution(task, new ExecutionListener() {

            @Override
            public void notifyEnd(Invocation invocation, boolean success) {
                sendTaskDone(invocation, success);
            }
        });
        executionManager.enqueue(e);

        // Notify the master that the data has been transfered
        // The message is sent after the task enqueue because the connection can
        // have N pending task transfer and will wait until they
        // are finished to send all the answers (blocking the task execution)
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("Notifying presence of all data for job " + task.getJobId() + ".");
        }

        CommandDataReceived cdr = new CommandDataReceived(this, task.getTransferGroupId());
        Connection c = TM.startConnection(masterNode);
        c.sendCommand(cdr);
        c.finishConnection();
    }

    /**
     * Remove obsolete objects.
     * 
     * @param obsolete List of obsolete objects.
     */
    public void removeObsolete(List<String> obsolete) {
        dataManager.removeObsoletes(obsolete);
    }

    @Override
    public void shutdownExecutionManager(Connection closingConnection) {
        // Stop the Execution Manager
        WORKER_LOGGER.debug("Stopping Execution Manager...");
        executionManager.stop();

        if (closingConnection != null) {
            closingConnection.sendCommand(new CommandExecutorShutdownACK());
            closingConnection.finishConnection();
        }
    }

    @Override
    public void shutdownExecutionManagerNotification(Connection c) {
        ErrorManager.warn("Shutdown execution ACK notification should never be received by a worker");
    }

    // Shutdown the worker, at this point there are no active transfers
    @Override
    public void shutdown(Connection closingConnection) {
        WORKER_LOGGER.debug("Entering shutdown method on worker");

        // Stop the NIOData Manager
        dataManager.stop();

        // Finish the main thread
        if (closingConnection != null) {
            closingConnection.sendCommand(new CommandShutdownACK());
            closingConnection.finishConnection();
        }

        TM.shutdown(closingConnection);

        // Remove workingDir
        if (REMOVE_WD) {
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("Erasing Worker Sandbox WorkingDir: " + this.workingDir);
            }
            try {
                removeFolder(this.workingDir);
            } catch (IOException ioe) {
                WORKER_LOGGER.error("Exception", ioe);
            }
        }

        WORKER_LOGGER.debug("Finish shutdown method on worker");
    }

    private void removeFolder(String sandBox) throws IOException {
        File wdirFile = new File(sandBox);
        remove(wdirFile);
    }

    private void remove(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                for (File child : f.listFiles()) {
                    remove(child);
                }
            }
            Files.delete(f.toPath());
        }
    }

    @Override
    public String getObjectAsFile(String s) {
        // This method should never be called in the worker side
        WORKER_LOGGER.warn("getObjectAsFile has been called in the worker side!");

        return null;
    }

    @Override
    public void receivedNIOTaskDone(Connection c, NIOTaskResult tr, boolean successful) {
        // Should not receive this call
    }

    @Override
    public void copiedData(int transfergroupID) {
        // Should not receive this call
    }

    @Override
    public void shutdownNotification(Connection c) {
        // Never orders the shutdown of a worker peer
    }

    @Override
    public void waitUntilTracingPackageGenerated() {
        // Nothing to do
    }

    @Override
    public void notifyTracingPackageGeneration() {
        // Nothing to do
    }

    @Override
    public void waitUntilWorkersDebugInfoGenerated() {
        // Nothing to do
    }

    @Override
    public void notifyWorkersDebugInfoGeneration() {
        // Nothing to do
    }

    @Override
    public void generateWorkersDebugInfo(Connection c) {
        // Freeze output
        String outSourcePath = workingDir + File.separator + "log" + File.separator + "worker_" + hostName + ".out";
        String outTarget = workingDir + File.separator + "log" + File.separator + "static_" + "worker_" + hostName
                + ".out";
        freezeFile(outSourcePath, outTarget);

        // Freeze error
        String errSourcePath = workingDir + File.separator + "log" + File.separator + "worker_" + hostName + ".err";
        String errTarget = workingDir + File.separator + "log" + File.separator + "static_" + "worker_" + hostName
                + ".err";
        freezeFile(errSourcePath, errTarget);

        // End
        c.sendCommand(new CommandWorkerDebugFilesDone());
        c.finishConnection();
    }

    private void freezeFile(String sourcePath, String targetPath) {
        File source = new File(sourcePath);
        if (source.exists()) {
            try {
                Files.copy(source.toPath(), new File(targetPath).toPath());
            } catch (Exception e) {
                WORKER_LOGGER.error("Exception", e);
            }
        } else {
            // TODO: Sending a file with "Empty file" is a patch because the comm library fails when transferring an
            // empty file
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(targetPath);
                fos.write("Empty file".getBytes());
                fos.close();
            } catch (Exception e) {
                WORKER_LOGGER.error("Exception", e);
            } finally {
                if (fos != null) {
                    try {
                        fos.close();
                    } catch (Exception e) {
                        WORKER_LOGGER.error("Exception", e);
                    }
                }
            }
        }
    }

    // **************************************
    // *************** MAIN *****************
    // **************************************

    /**
     * Entry Point.
     * 
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        // Check arguments length
        if (args.length != (NUM_PARAMS_NIO_WORKER)) {
            WORKER_LOGGER.debug("Received parameters: ");
            for (int i = 0; i < args.length; ++i) {
                WORKER_LOGGER.debug("Param " + i + ":  " + args[i]);
            }
            ErrorManager.fatal(ERROR_INCORRECT_NUM_PARAMS);
        }

        // Parse arguments
        boolean debug = Boolean.valueOf(args[0]);

        int maxSnd = Integer.parseInt(args[1]);
        int maxRcv = Integer.parseInt(args[2]);
        String workerIP = args[3];
        int wPort = Integer.parseInt(args[4]);
        String mName = args[5];
        int mPort = Integer.parseInt(args[6]);
        int streamingPort = Integer.parseInt(args[7]);

        int computingUnitsCPU = Integer.parseInt(args[8]);
        int computingUnitsGPU = Integer.parseInt(args[9]);
        int computingUnitsFPGA = Integer.parseInt(args[10]);
        String cpuMap = args[11];
        String gpuMap = args[12];
        String fpgaMap = args[13];
        int limitOfTasks = Integer.parseInt(args[14]);

        String appUuid = args[15];
        // String lang = args[16];
        String workingDir = args[17];
        String installDir = args[18];
        final String appDir = args[19];
        String libPath = args[20];
        String classpath = args[21];
        String pythonpath = args[22];

        String traceFlag = args[23];
        String extraeFile = args[24];
        String traceHost = args[25];

        String storageConf = args[26];
        TaskExecution executionType = TaskExecution.valueOf(args[27].toUpperCase());

        boolean persistentC = Boolean.parseBoolean(args[28]);

        String pythonInterpreter = args[29];
        String pythonVersion = args[30];
        String pythonVirtualEnvironment = args[31];
        String pythonPropagateVirtualEnvironment = args[32];
        String pythonMpiWorker = args[33];

        final JavaParams javaParams = new JavaParams(classpath);
        final PythonParams pyParams = new PythonParams(pythonInterpreter, pythonVersion, pythonVirtualEnvironment,
                pythonPropagateVirtualEnvironment, pythonpath, pythonMpiWorker);
        final CParams cParams = new CParams(classpath);

        // Print arguments
        if (WORKER_LOGGER.isDebugEnabled()) {
            WORKER_LOGGER.debug("maxSnd: " + String.valueOf(maxSnd));
            WORKER_LOGGER.debug("maxRcv: " + String.valueOf(maxRcv));

            WORKER_LOGGER.debug("WorkerName: " + workerIP);
            WORKER_LOGGER.debug("WorkerPort: " + String.valueOf(wPort));
            WORKER_LOGGER.debug("MasterName: " + mName);
            WORKER_LOGGER.debug("MasterPort: " + String.valueOf(mPort));
            WORKER_LOGGER.debug("StreamingPort: " + String.valueOf(streamingPort));

            WORKER_LOGGER.debug("Computing Units CPU: " + String.valueOf(computingUnitsCPU));
            WORKER_LOGGER.debug("Computing Units GPU: " + String.valueOf(computingUnitsGPU));
            WORKER_LOGGER.debug("Computing Units FPGA: " + String.valueOf(computingUnitsFPGA));
            WORKER_LOGGER.debug("User defined CPU Map: " + cpuMap);
            WORKER_LOGGER.debug("User defined GPU Map: " + gpuMap);
            WORKER_LOGGER.debug("User defined FPGA Map: " + fpgaMap);
            WORKER_LOGGER.debug("Limit Of Tasks: " + String.valueOf(limitOfTasks));

            WORKER_LOGGER.debug("App uuid: " + appUuid);
            WORKER_LOGGER.debug("WorkingDir:" + workingDir);
            WORKER_LOGGER.debug("Install Dir: " + installDir);

            WORKER_LOGGER.debug("Tracing: " + traceFlag);
            WORKER_LOGGER.debug("Extrae config File: " + extraeFile);
            WORKER_LOGGER.debug("Host: " + traceHost);

            WORKER_LOGGER.debug("LibraryPath: " + libPath);
            WORKER_LOGGER.debug("Classpath: " + classpath);
            WORKER_LOGGER.debug("Pythonpath: " + pythonpath);

            WORKER_LOGGER.debug("StorageConf: " + storageConf);
            WORKER_LOGGER.debug("executionType: " + executionType);

            WORKER_LOGGER.debug("Persistent c: " + persistentC);

            WORKER_LOGGER.debug("Python interpreter: " + pythonInterpreter);
            WORKER_LOGGER.debug("Python version: " + pythonVersion);
            WORKER_LOGGER.debug("Python virtual environment: " + pythonVirtualEnvironment);
            WORKER_LOGGER.debug("Python propagate virtual environment: " + pythonPropagateVirtualEnvironment);
            WORKER_LOGGER.debug("Python use MPI worker: " + pythonMpiWorker);

            WORKER_LOGGER.debug("Remove Sanbox WD: " + REMOVE_WD);
        }

        // Configure storage
        System.setProperty(COMPSsConstants.STORAGE_CONF, storageConf);

        // Configure tracing
        System.setProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, extraeFile);

        /*
         * ***********************************************************************************************************
         * LAUNCH THE WORKER
         *************************************************************************************************************/
        NIOWorker nw = new NIOWorker(debug, maxSnd, maxRcv, workerIP, mName, mPort, streamingPort, computingUnitsCPU,
                computingUnitsGPU, computingUnitsFPGA, cpuMap, gpuMap, fpgaMap, limitOfTasks, appUuid, traceFlag,
                traceHost, storageConf, executionType, persistentC, workingDir, installDir, appDir, javaParams,
                pyParams, cParams);

        NIOMessageHandler mh = new NIOMessageHandler(nw);

        // Initialize the Transfer Manager
        WORKER_LOGGER.debug("  Initializing the TransferManager structures...");
        try {
            TM.init(NIO_EVENT_MANAGER_CLASS, null, mh);
        } catch (CommException ce) {
            WORKER_LOGGER.error("Error initializing Transfer Manager on worker " + nw.getHostName(), ce);
            // Shutdown the Worker since the error it is not recoverable
            nw.shutdown(null);
            return;
        }

        // Start the Transfer Manager thread (starts the EventManager)
        // WORKER_LOGGER.debug(" Starting TransferManager Thread");
        // TM.start();
        try {
            TM.startServer(new NIONode(null, wPort));
        } catch (CommException ce) {
            WORKER_LOGGER.error("Error starting TransferManager Server at Worker" + nw.getHostName(), ce);
            nw.shutdown(null);
            return;
        }

        if (NIOTracer.extraeEnabled()) {
            NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.START.getType());
        }

        /*
         * ***********************************************************************************************************
         * JOIN AND END
         *************************************************************************************************************/
        // Wait for the Transfer Manager thread to finish (the shutdown is received on that thread)
        try {
            TM.join();
        } catch (InterruptedException ie) {
            WORKER_LOGGER.warn("TransferManager interrupted", ie);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void receivedBindingObjectAsFile(String filename, String target) {
        // Nothing to do at worker
    }

    @Override
    protected boolean isMaster() {
        return false;
    }

    // ********************************************************************
    // *************** INVOCATION CONTEXT IMPLEMENTATIONS *****************
    // ********************************************************************
    // WORKER CONFIGURATION
    @Override
    public String getHostName() {
        return this.host;
    }

    @Override
    public long getTracingHostID() {
        return Long.parseLong(NIOTracer.getHostID());
    }

    @Override
    public String getAppDir() {
        return this.appDir;
    }

    @Override
    public String getInstallDir() {
        return this.installDir;
    }

    @Override
    public String getWorkingDir() {
        return workingDir;
    }

    // EXECUTION CONFIGURATION
    @Override
    public COMPSsConstants.TaskExecution getExecutionType() {
        return executionType;
    }

    @Override
    public LanguageParams getLanguageParams(Lang lang) {
        WORKER_LOGGER
                .info("GETTING LANGUAGE PARAMS :" + Lang.PYTHON.ordinal() + " -> " + this.langParams[lang.ordinal()]);
        return this.langParams[lang.ordinal()];
    }

    // EXECUTION MANAGEMENT
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
        return this.getWorkingDir() + "jobs" + File.separator + "job" + invocation.getJobId() + "_"
                + invocation.getHistory();
    }

    /**
     * Get the stderr stream assigned to this computing thread
     *
     * @return
     */
    @Override
    public PrintStream getThreadErrStream() {
        return err.getStream();
    }

    /**
     * Get the stdout stream assigned to this computing thread
     *
     * @return
     */
    @Override
    public PrintStream getThreadOutStream() {
        return out.getStream();
    }

    // DATA MANAGEMENT
    @Override
    public String getStorageConf() {
        return dataManager.getStorageConf();
    }

    @Override
    public void loadParam(InvocationParam param) throws Exception {
        dataManager.loadParam(param);
    }

    @Override
    public void storeParam(InvocationParam param) {
        dataManager.storeParam(param);
    }

    @Override
    public Object getObject(String name) {
        String realName = name.substring(name.lastIndexOf('/') + 1);
        return dataManager.getObject(realName);
    }

    @Override
    public void increaseResources(MethodResourceDescription description) {
        int cpuCount = description.getTotalCPUComputingUnits();
        int GPUCount = description.getTotalGPUComputingUnits();
        int FPGACount = description.getTotalFPGAComputingUnits();
        int otherCount = description.getTotalOTHERComputingUnits();
        executionManager.increaseCapabilities(cpuCount, GPUCount, FPGACount, otherCount);
    }

    @Override
    public void reduceResources(MethodResourceDescription description) {
        int cpuCount = description.getTotalCPUComputingUnits();
        int GPUCount = description.getTotalGPUComputingUnits();
        int FPGACount = description.getTotalFPGAComputingUnits();
        int otherCount = description.getTotalOTHERComputingUnits();
        executionManager.reduceCapabilities(cpuCount, GPUCount, FPGACount, otherCount);
    }

    @Override
    public void performedResourceUpdate(Connection c) {
        // Should never request a resourceModification
    }


    public class FetchDataOperationListener extends MultiOperationFetchListener {

        private final int transferId;


        public FetchDataOperationListener(int transferId) {
            this.transferId = transferId;
        }

        @Override
        public void doCompleted() {
            CommandDataReceived cdr = new CommandDataReceived(NIOWorker.this, transferId);
            Connection c = TM.startConnection(masterNode);
            c.sendCommand(cdr);
            c.finishConnection();
        }

        @Override
        public void doFailure(String failedDataId, Exception cause) {

        }

    }

    public class TaskFetchOperationsListener extends MultiOperationFetchListener {

        private final NIOTask task;


        public TaskFetchOperationsListener(NIOTask task) {
            super();
            this.task = task;
        }

        public NIOTask getTask() {
            return this.task;
        }

        @Override
        public void doCompleted() {
            Long stTime = times.get(task.getJobId());
            if (stTime != null) {
                long duration = System.currentTimeMillis() - stTime;
                WORKER_LOGGER.info(" [Profile] Transfer: " + duration);
            }
            executeTask(task);
        }

        @Override
        public void doFailure(String failedDataId, Exception cause) {
            // Create job*_[NEW|RESUBMITTED|RESCHEDULED].[out|err]
            // If we don't create this when the task fails to retrieve a value,
            // the master will try to get the out of this job, and it will get blocked.
            // Same for the worker when sending, throwing an error when trying
            // to read the job out, which wouldn't exist
            String baseJobPath = getStandardStreamsPath(task);
            String errorMessage = "Worker closed because the data " + failedDataId + " couldn't be retrieved.";
            String taskFileOutName = baseJobPath + ".out";
            checkStreamFileExistence(taskFileOutName, "out", errorMessage);
            String taskFileErrName = baseJobPath + ".err";
            checkStreamFileExistence(taskFileErrName, "err", errorMessage);
            sendTaskDone(task, false);
        }

    }

}
