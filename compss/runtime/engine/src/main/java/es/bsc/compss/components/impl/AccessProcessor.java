/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.components.impl;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.exceptions.CannotLoadException;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.data.AccessParams;
import es.bsc.compss.types.data.AccessParams.AccessMode;
import es.bsc.compss.types.data.AccessParams.FileAccessParams;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.RAccessId;
import es.bsc.compss.types.data.DataAccessId.RWAccessId;
import es.bsc.compss.types.data.DataAccessId.WAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.ResultFile;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.types.request.ap.DeleteBindingObjectRequest;
import es.bsc.compss.types.request.ap.FinishBindingObjectAccessRequest;
import es.bsc.compss.types.request.ap.FinishFileAccessRequest;
import es.bsc.compss.types.request.ap.TransferBindingObjectRequest;
import es.bsc.compss.types.request.ap.TransferRawFileRequest;
import es.bsc.compss.types.request.ap.AlreadyAccessedRequest;
import es.bsc.compss.types.request.ap.GetResultFilesRequest;
import es.bsc.compss.types.request.ap.DeleteFileRequest;
import es.bsc.compss.types.request.ap.EndOfAppRequest;
import es.bsc.compss.types.request.ap.GetLastRenamingRequest;
import es.bsc.compss.types.request.ap.TaskEndNotification;
import es.bsc.compss.types.request.ap.IsObjectHereRequest;
import es.bsc.compss.types.request.ap.NewVersionSameValueRequest;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.ap.SetObjectVersionValueRequest;
import es.bsc.compss.types.request.ap.ShutdownRequest;
import es.bsc.compss.types.request.ap.APRequest;
import es.bsc.compss.types.request.ap.TaskAnalysisRequest;
import es.bsc.compss.types.request.ap.TasksStateRequest;
import es.bsc.compss.types.request.ap.TransferObjectRequest;
import es.bsc.compss.types.request.ap.TransferOpenFileRequest;
import es.bsc.compss.types.request.ap.UnblockResultFilesRequest;
import es.bsc.compss.types.request.ap.WaitForConcurrentRequest;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.WaitForTaskRequest;
import es.bsc.compss.types.request.ap.DeregisterObject;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;


/**
 * Component to handle the tasks accesses to files and object
 */
public class AccessProcessor implements Runnable, TaskProducer {

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String ERROR_OBJECT_LOAD_FROM_STORAGE = "ERROR: Cannot load object from storage (file or PSCO)";
    private static final String ERROR_QUEUE_OFFER = "ERROR: AccessProcessor queue offer error on ";

    // Other super-components
    protected TaskDispatcher taskDispatcher;

    // Subcomponents
    protected TaskAnalyser taskAnalyser;
    protected DataInfoProvider dataInfoProvider;

    // Processor thread
    private static Thread processor;
    private static boolean keepGoing;

    // Tasks to be processed
    protected LinkedBlockingQueue<APRequest> requestQueue;


    /**
     * Creates a new Access Processor instance
     *
     * @param td
     */
    public AccessProcessor(TaskDispatcher td) {
        taskDispatcher = td;

        // Start Subcomponents
        taskAnalyser = new TaskAnalyser();
        dataInfoProvider = new DataInfoProvider();

        taskAnalyser.setCoWorkers(dataInfoProvider);
        requestQueue = new LinkedBlockingQueue<>();

        keepGoing = true;
        processor = new Thread(this);
        processor.setName("Access Processor");
        if (Tracer.basicModeEnabled()) {
            Tracer.enablePThreads();
        }
        processor.start();
        if (Tracer.basicModeEnabled()) {
            Tracer.disablePThreads();
        }
    }

    /**
     * Sets the GraphGenerator co-worker
     *
     * @param gm
     */
    public void setGM(GraphGenerator gm) {
        this.taskAnalyser.setGM(gm);
    }

    @Override
    public void run() {
        while (keepGoing) {
            APRequest request = null;
            try {
                request = requestQueue.take();

                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.getAPRequestEvent(request.getRequestType().name()).getId(), Tracer.getRuntimeEventsType());
                }
                request.process(this, taskAnalyser, dataInfoProvider, taskDispatcher);
                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }

            } catch (ShutdownException se) {
                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                se.getSemaphore().release();
                break;
            } catch (Exception e) {
                LOGGER.error("Exception", e);
                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
            }
        }

        LOGGER.info("AccessProcessor shutdown");
    }

    /**
     * App : new Method Task
     *
     * @param appId
     * @param monitor
     * @param lang
     * @param signature
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param numReturns
     * @param hasTarget
     * @param parameters
     * @return
     */
    public int newTask(Long appId, TaskMonitor monitor, Lang lang, String signature, boolean isPrioritary, int numNodes,
            boolean isReplicated, boolean isDistributed, boolean hasTarget, int numReturns, Parameter[] parameters) {

        Task currentTask = new Task(appId, lang, signature, isPrioritary, numNodes, isReplicated, isDistributed,
                hasTarget, numReturns, parameters, monitor);
        TaskMonitor registeredMonitor = currentTask.getTaskMonitor();
        registeredMonitor.onCreation();
        if (!requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new method task");
        }
        return currentTask.getId();
    }

    /**
     * App : new Service task
     *
     * @param appId
     * @param monitor
     * @param namespace
     * @param service
     * @param port
     * @param operation
     * @param priority
     * @param hasTarget
     * @param numReturns
     * @param parameters
     * @return
     */
    public int newTask(Long appId, TaskMonitor monitor, String namespace, String service, String port, String operation,
            boolean priority, boolean hasTarget, int numReturns, Parameter[] parameters) {

        Task currentTask = new Task(appId, namespace, service, port, operation, priority, hasTarget, numReturns,
                parameters, monitor);
        TaskMonitor registeredMonitor = currentTask.getTaskMonitor();
        registeredMonitor.onCreation();
        if (!requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new service task");
        }
        return currentTask.getId();
    }

    // Notification thread (JM)
    @Override
    public void notifyTaskEnd(Task task) {
        if (!requestQueue.offer(new TaskEndNotification(task))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "notify task end");
        }
    }

    public void finishAccessToFile(DataLocation sourceLocation, AccessParams.FileAccessParams fap, String destDir) {
        boolean alreadyAccessed = alreadyAccessed(sourceLocation);

        if (!alreadyAccessed) {
            LOGGER.debug("File not accessed before. Nothing to do");
            return;
        }

        // Tell the DM that the application wants to access a file.
        finishFileAccess(fap);

    }

    private void finishFileAccess(FileAccessParams fap) {
        if (!requestQueue.offer(new FinishFileAccessRequest(fap))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "finishing file access");
        }
    }

    /**
     * Notifies a main access to a given file @sourceLocation in mode @fap
     *
     * @param sourceLocation
     * @param fap
     * @param destDir
     * @return
     */
    public DataLocation mainAccessToFile(DataLocation sourceLocation, AccessParams.FileAccessParams fap,
            String destDir) {
        boolean alreadyAccessed = alreadyAccessed(sourceLocation);

        if (!alreadyAccessed) {
            LOGGER.debug("File not accessed before, returning the same location");
            return sourceLocation;
        }

        // Tell the DM that the application wants to access a file.
        DataAccessId faId = registerDataAccess(fap);
        DataLocation tgtLocation = sourceLocation;

        if (fap.getMode() != AccessMode.W) {
            // Wait until the last writer task for the file has finished
            LOGGER.debug("File " + faId.getDataId() + " mode contains R, waiting until the last writer has finished");

            waitForTask(faId.getDataId(), AccessMode.R);
            if (taskAnalyser.dataWasAccessedConcurrent(faId.getDataId())) {
                waitForConcurrent(faId.getDataId(), fap.getMode());
                taskAnalyser.removeFromConcurrentAccess(faId.getDataId());
            }
            if (destDir == null) {
                tgtLocation = transferFileOpen(faId);
            } else {
                DataInstanceId daId;
                if (fap.getMode() == AccessMode.R) {
                    RAccessId ra = (RAccessId) faId;
                    daId = ra.getReadDataInstance();
                } else {
                    RWAccessId ra = (RWAccessId) faId;
                    daId = ra.getReadDataInstance();
                }

                String rename = daId.getRenaming();
                String path = DataLocation.Protocol.FILE_URI.getSchema() + destDir + rename;
                try {
                    SimpleURI uri = new SimpleURI(path);
                    tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
                }

                transferFileRaw(faId, tgtLocation);
            }
        }

        if (fap.getMode() != AccessMode.R && fap.getMode() != AccessMode.C) {
            // Mode contains W
            LOGGER.debug("File " + faId.getDataId() + " mode contains W, register new writer");
            DataInstanceId daId;
            if (fap.getMode() == AccessMode.RW) {
                RWAccessId ra = (RWAccessId) faId;
                daId = ra.getWrittenDataInstance();
            } else {
                WAccessId ra = (WAccessId) faId;
                daId = ra.getWrittenDataInstance();
            }
            String rename = daId.getRenaming();
            String path = DataLocation.Protocol.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + rename;
            try {
                SimpleURI uri = new SimpleURI(path);
                tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }
            Comm.registerLocation(rename, tgtLocation);
        }

        if (DEBUG) {
            LOGGER.debug("File " + faId.getDataId() + " located on " + tgtLocation.toString());
        }
        return tgtLocation;
    }

    /**
     * Returns if the value with hashCode @hashCode is valid or obsolete
     *
     * @param hashCode
     * @return
     */
    public boolean isCurrentRegisterValueValid(int hashCode) {
        LOGGER.debug("Checking if value of object with hashcode " + hashCode + " is valid");

        Semaphore sem = new Semaphore(0);
        IsObjectHereRequest request = new IsObjectHereRequest(hashCode, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "valid object value");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        // Log response and return
        boolean isValid = request.getResponse();
        if (DEBUG) {
            if (isValid) {
                LOGGER.debug("Value of object with hashcode " + hashCode + " is valid");
            } else {
                LOGGER.debug("Value of object with hashcode " + hashCode + " is NOT valid");
            }
        }

        return isValid;
    }

    /**
     * Notifies a main access to an object @obj
     *
     * @param obj
     * @param hashCode
     * @return
     */
    public Object mainAcessToObject(Object obj, int hashCode) {
        if (DEBUG) {
            LOGGER.debug("Requesting main access to object with hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        AccessParams.ObjectAccessParams oap = new AccessParams.ObjectAccessParams(AccessMode.RW, obj, hashCode);
        DataAccessId oaId = registerDataAccess(oap);
        DataInstanceId wId = ((DataAccessId.RWAccessId) oaId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        // Wait until the last writer task for the object has finished
        if (DEBUG) {
            LOGGER.debug("Waiting for last writer of " + oaId.getDataId() + " with renaming " + wRename);
        }

        // Defaut access is read because the binding object is removed after accessing it
        waitForTask(oaId.getDataId(), AccessMode.RW);
        if (taskAnalyser.dataWasAccessedConcurrent(oaId.getDataId())) {
            waitForConcurrent(oaId.getDataId(), AccessMode.RW);
            if (oaId.getDirection() != DataAccessId.Direction.R || oaId.getDirection() != DataAccessId.Direction.RW) {
                taskAnalyser.removeFromConcurrentAccess(oaId.getDataId());
            }
        }
        // TODO: Check if the object was already piggybacked in the task notification
        // Ask for the object
        if (DEBUG) {
            LOGGER.debug("Request object transfer " + oaId.getDataId() + " with renaming " + wRename);
        }
        Object oUpdated = obtainObject(oaId);

        if (DEBUG) {
            LOGGER.debug("Object retrieved. Set new version to: " + wRename);
        }
        setObjectVersionValue(wRename, oUpdated);
        return oUpdated;
    }

    /**
     * Notifies a main access to an external PSCO {@code id}
     *
     * @param fileName
     * @param id
     * @param hashCode
     * @return
     */
    public String mainAcessToExternalPSCO(String id, int hashCode) {
        if (DEBUG) {
            LOGGER.debug("Requesting main access to external object with hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        AccessParams.ObjectAccessParams oap = new AccessParams.ObjectAccessParams(AccessMode.RW, id, hashCode);
        DataAccessId oaId = registerDataAccess(oap);
        DataInstanceId wId = ((DataAccessId.RWAccessId) oaId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        // Wait until the last writer task for the object has finished
        if (DEBUG) {
            LOGGER.debug("Waiting for last writer of " + oaId.getDataId() + " with renaming " + wRename);
        }

        waitForTask(oaId.getDataId(), AccessMode.RW);
        if (taskAnalyser.dataWasAccessedConcurrent(oaId.getDataId())) {
            waitForConcurrent(oaId.getDataId(), AccessMode.RW);
            if (oaId.getDirection() != DataAccessId.Direction.R || oaId.getDirection() != DataAccessId.Direction.RW) {
                taskAnalyser.removeFromConcurrentAccess(oaId.getDataId());
            }
        }

        // TODO: Check if the object was already piggybacked in the task notification
        String lastRenaming = ((DataAccessId.RWAccessId) oaId).getReadDataInstance().getRenaming();
        String newId = Comm.getData(lastRenaming).getPscoId();

        return Protocol.PERSISTENT_URI.getSchema() + newId;
    }

    private String obtainBindingObject(RAccessId oaId) {
        // String lastRenaming = (oaId).getReadDataInstance().getRenaming();
        // TODO: Add transfer request similar than java object
        LOGGER.debug("[AccessProcessor] Obtaining binding object with id " + oaId);
        // Ask for the object
        Semaphore sem = new Semaphore(0);
        TransferBindingObjectRequest tor = new TransferBindingObjectRequest(oaId, sem);
        if (!requestQueue.offer(tor)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "obtain object");
        }

        // Wait for response
        sem.acquireUninterruptibly();
        BindingObject bo = BindingObject.generate(tor.getTargetName());
        return bo.getName();
    }

    /**
     * Notifies a main access to an external PSCO {@code id}
     *
     * @param fileName
     * @param id
     * @param hashCode
     * @return
     */
    public String mainAcessToBindingObject(BindingObject bo, int hashCode) {
        if (DEBUG) {
            LOGGER.debug(
                    "Requesting main access to binding object with bo " + bo.toString() + " and hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        // AccessParams.BindingObjectAccessParams oap = new AccessParams.BindingObjectAccessParams(AccessMode.RW, bo,
        // hashCode);
        AccessParams.BindingObjectAccessParams oap = new AccessParams.BindingObjectAccessParams(AccessMode.R, bo,
                hashCode);
        DataAccessId oaId = registerDataAccess(oap);

        // DataInstanceId wId = ((DataAccessId.RWAccessId) oaId).getWrittenDataInstance();
        // String wRename = wId.getRenaming();
        // Wait until the last writer task for the object has finished
        if (DEBUG) {
            LOGGER.debug("Waiting for last writer of " + oaId.getDataId());
        }

        // Defaut access is read because the binding object is removed after accessing it
        waitForTask(oaId.getDataId(), AccessMode.R);
        if (taskAnalyser.dataWasAccessedConcurrent(oaId.getDataId())) {
            // Defaut access is read because the binding object is removed after accessing it
            waitForConcurrent(oaId.getDataId(), AccessMode.R);
            if (oaId.getDirection() != DataAccessId.Direction.R || oaId.getDirection() != DataAccessId.Direction.RW) {
                taskAnalyser.removeFromConcurrentAccess(oaId.getDataId());
            }
        }
        // String lastRenaming = ((DataAccessId.RWAccessId) oaId).getReadDataInstance().getRenaming();
        // return obtainBindingObject((DataAccessId.RWAccessId)oaId);
        String bindingObjectID = obtainBindingObject((DataAccessId.RAccessId) oaId);

        finishBindingObjectAccess(oap);

        return bindingObjectID;
    }

    private void finishBindingObjectAccess(AccessParams.BindingObjectAccessParams boAP) {
        if (!requestQueue.offer(new FinishBindingObjectAccessRequest(boAP))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "finishing binding object access");
        }
    }

    /**
     * Notification for no more tasks
     *
     * @param appId
     */
    public void noMoreTasks(Long appId) {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new EndOfAppRequest(appId, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "no more tasks");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.info("All tasks finished");
    }

    /**
     * Returns whether the @loc has already been accessed or not
     *
     * @param loc
     * @return
     */
    private boolean alreadyAccessed(DataLocation loc) {
        Semaphore sem = new Semaphore(0);
        AlreadyAccessedRequest request = new AlreadyAccessedRequest(loc, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "already accessed location");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Barrier
     *
     * @param appId
     */
    public void barrier(Long appId) {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new BarrierRequest(appId, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for all tasks");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.info("Barrier: End of waited all tasks");
    }

    /**
     * Synchronism for an specific task
     *
     * @param dataId
     * @param mode
     */
    private void waitForTask(int dataId, AccessMode mode) {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new WaitForTaskRequest(dataId, mode, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for task");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.info("End of waited task for data " + dataId);
    }

    /**
     * Synchronism for a concurrent task
     *
     * @param dataId
     * @param accessMode
     */
    private void waitForConcurrent(int dataId, AccessMode accessMode) {
        Semaphore sem = new Semaphore(0);
        Semaphore semTasks = new Semaphore(0);
        WaitForConcurrentRequest request = new WaitForConcurrentRequest(dataId, accessMode, sem, semTasks);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for concurrent task");
        }

        // Wait for response
        sem.acquireUninterruptibly();
        int n = request.getNumWaitedTasks();
        semTasks.acquireUninterruptibly(n);
        LOGGER.info("End of waited concurrent task for data " + dataId);
    }

    /**
     * Registers a new data access
     *
     * @param access
     * @return
     */
    private DataAccessId registerDataAccess(AccessParams access) {
        Semaphore sem = new Semaphore(0);
        RegisterDataAccessRequest request = new RegisterDataAccessRequest(access, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "register data access");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Registers a new version of file/object with the same value
     *
     * @param rRenaming
     * @param wRenaming
     */
    public void newVersionSameValue(String rRenaming, String wRenaming) {
        NewVersionSameValueRequest request = new NewVersionSameValueRequest(rRenaming, wRenaming);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new version");
        }
    }

    /**
     * Sets a new value to a specific version of a file/object
     *
     * @param renaming
     * @param value
     */
    public void setObjectVersionValue(String renaming, Object value) {
        SetObjectVersionValueRequest request = new SetObjectVersionValueRequest(renaming, value);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new object version value");
        }
    }

    /**
     * Returns the last version of a file/object with code @code
     *
     * @param code
     * @return
     */
    public String getLastRenaming(int code) {
        Semaphore sem = new Semaphore(0);
        GetLastRenamingRequest request = new GetLastRenamingRequest(code, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get last renaming");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Unblock result files
     *
     * @param resFiles
     */
    public void unblockResultFiles(List<ResultFile> resFiles) {
        UnblockResultFilesRequest request = new UnblockResultFilesRequest(resFiles);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unblock result files");
        }
    }

    /**
     * Shutdown request
     */
    public void shutdown() {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new ShutdownRequest(sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "shutdown");
        }

        // Wait for response
        sem.acquireUninterruptibly();
    }

    /**
     * Returns a string with the description of the tasks in the graph
     *
     * @return description of the current tasks in the graph
     */
    public String getCurrentTaskState() {
        Semaphore sem = new Semaphore(0);
        TasksStateRequest request = new TasksStateRequest(sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get current task state");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return (String) request.getResponse();
    }

    /**
     * Marks a location for deletion
     *
     * @param loc
     */
    public void markForDeletion(DataLocation loc) {
        LOGGER.debug("Marking data " + loc + " for deletion");
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new DeleteFileRequest(loc, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "mark for deletion");
        }
        // Wait for response

        sem.acquireUninterruptibly();
        LOGGER.debug("Sata " + loc + " deleted");
    }

    /**
     * Marks a location for deletion
     *
     * @param loc
     */
    public void markForBindingObjectDeletion(int code) {
        if (!requestQueue.offer(new DeleteBindingObjectRequest(code))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "mark for deletion");
        }
    }

    /**
     * Adds a request for file raw transfer
     *
     * @param faId
     * @param location
     */
    private void transferFileRaw(DataAccessId faId, DataLocation location) {
        Semaphore sem = new Semaphore(0);
        TransferRawFileRequest request = new TransferRawFileRequest((RAccessId) faId, location, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file raw");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Raw file transferred");
    }

    /**
     * Adds a request for open file transfer
     *
     * @param faId
     * @return
     */
    private DataLocation transferFileOpen(DataAccessId faId) {
        Semaphore sem = new Semaphore(0);
        TransferOpenFileRequest request = new TransferOpenFileRequest(faId, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file open");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Open file transferred");
        return request.getLocation();
    }

    /**
     * Adds a request to obtain an object from a worker to the master
     *
     * @param oaId
     * @return
     */
    private Object obtainObject(DataAccessId oaId) {
        // Ask for the object
        Semaphore sem = new Semaphore(0);
        TransferObjectRequest tor = new TransferObjectRequest(oaId, sem);
        if (!requestQueue.offer(tor)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "obtain object");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        // Get response
        Object oUpdated = tor.getResponse();
        if (oUpdated == null) {
            /*
             * The Object didn't come from a WS but was transferred from a worker, we load it from its storage (file or
             * persistent)
             */
            LogicalData ld = tor.getTargetData();
            try {
                ld.loadFromStorage();
                oUpdated = ld.getValue();
            } catch (CannotLoadException e) {
                LOGGER.fatal(ERROR_OBJECT_LOAD_FROM_STORAGE + ": " + ((ld == null) ? "null" : ld.getName()), e);
                ErrorManager.fatal(ERROR_OBJECT_LOAD_FROM_STORAGE + ": " + ((ld == null) ? "null" : ld.getName()), e);
            }
        }

        return oUpdated;
    }

    /**
     * Adds a request to retrieve the result files from the workers to the master
     *
     * @param appId
     */
    public void getResultFiles(Long appId) {
        Semaphore sem = new Semaphore(0);
        GetResultFilesRequest request = new GetResultFilesRequest(appId, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get result files");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        UnblockResultFilesRequest urfr = new UnblockResultFilesRequest(request.getBlockedData());
        if (!requestQueue.offer(urfr)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unlock result files");
        }
    }

    /**
     * Deregister the given object
     *
     * @param o
     */
    public void deregisterObject(Object o) {

        if (!requestQueue.offer(new DeregisterObject(o))) {

            ErrorManager.error(ERROR_QUEUE_OFFER + "deregister object");
        }

    }

}
