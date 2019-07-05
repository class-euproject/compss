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
package es.bsc.compss.types.resources;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.listener.SafeCopyListener;
import es.bsc.compss.types.data.listener.TracingCopyListener;
import es.bsc.compss.types.data.listener.WorkersDebugInformationListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.LocationType;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.transferable.SafeCopyTransferable;
import es.bsc.compss.types.data.transferable.TracingCopyTransferable;
import es.bsc.compss.types.data.transferable.WorkersDebugInfoCopyTransferable;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.SharedDiskManager;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class ResourceImpl implements Comparable<Resource>, Resource {

    // Log and debug
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final boolean DEBUG = LOGGER.isDebugEnabled();

    protected final String name;
    private final COMPSsNode node;
    protected Map<String, String> sharedDisks;

    private final List<LogicalData> obsoletes = new LinkedList<>();
    private final Set<LogicalData> privateFiles = new HashSet<>();

    public ResourceImpl(String name, Configuration conf, Map<String, String> sharedDisks) {
        this.name = name;
        this.node = Comm.initWorker(conf);
        this.sharedDisks = sharedDisks;
        SharedDiskManager.addMachine(this);
        ResourcesPool.add(this);
    }

    public ResourceImpl(COMPSsNode node, Map<String, String> sharedDisks) {
        this.name = node.getName();
        this.node = node;
        this.sharedDisks = sharedDisks;
        SharedDiskManager.addMachine(this);
        ResourcesPool.add(this);
    }

    public ResourceImpl(ResourceImpl clone) {
        name = clone.name;
        node = clone.node;
        ResourcesPool.add(this);
    }

    /**
     * Starts a resource execution
     */
    @Override
    public void start() throws InitNodeException {
        this.node.start();
        if (sharedDisks != null) {
            for (Entry<String, String> disk : sharedDisks.entrySet()) {
                SharedDiskManager.addSharedToMachine(disk.getKey(), disk.getValue(), this);
            }
        }
    }

    /**
     * Returns all the LogicalData stored in the host
     *
     * @return
     */
    @Override
    public Set<LogicalData> getAllDataFromHost() {
        Set<LogicalData> data = new HashSet<>();

        List<String> sharedDisks = SharedDiskManager.getAllSharedNames(this);
        for (String diskName : sharedDisks) {
            Set<LogicalData> sharedData = SharedDiskManager.getAllSharedFiles(diskName);
            if (sharedData != null) {
                synchronized (sharedData) {
                    data.addAll(sharedData);
                }
            }
        }

        synchronized (privateFiles) {
            data.addAll(privateFiles);
        }

        return data;
    }

    /**
     * Adds a new LogicalData available in the host
     *
     * @param ld
     */
    @Override
    public void addLogicalData(LogicalData ld) {
        synchronized (privateFiles) {
            privateFiles.add(ld);
        }
    }

    /**
     * Marks a file as obsolete
     *
     * @param obsolete
     */
    @Override
    public final void addObsolete(LogicalData obsolete) {
        if (getType() == ResourceType.WORKER) {
            synchronized (obsoletes) {
                obsoletes.add(obsolete);
            }
        }

        // Remove from private files
        synchronized (privateFiles) {
            this.privateFiles.remove(obsolete);
        }

        // Remove from shared disk files
        List<String> sharedDisks = SharedDiskManager.getAllSharedNames(this);
        for (String diskName : sharedDisks) {
            SharedDiskManager.removeLogicalData(diskName, obsolete);
        }

    }

    /**
     * Gets the list of obsolete files
     *
     * @return List of logicalData objects
     */
    @Override
    public final LogicalData[] pollObsoletes() {
        synchronized (obsoletes) {
            LogicalData[] obs = obsoletes.toArray(new LogicalData[obsoletes.size()]);
            obsoletes.clear();
            return obs;
        }
    }

    /**
     * Clears the list of obsolete files
     */
    public final void clearObsoletes() {
        synchronized (obsoletes) {
            obsoletes.clear();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public COMPSsNode getNode() {
        return node;
    }

    @Override
    public void setInternalURI(MultiURI u) throws UnstartedNodeException {
        node.setInternalURI(u);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl,
            List<String> slaveWorkersNodeNames, JobListener listener) {

        return node.newJob(taskId, taskParams, impl, this, slaveWorkersNodeNames, listener);
    }

    @Override
    public void getData(String dataId, String tgtDataId, Transferable reason, EventListener listener) {
        LogicalData srcData = Comm.getData(dataId);
        LogicalData tgtData = null;
        if (tgtDataId != null) {
            tgtData = Comm.getData(tgtDataId);
        }
        getData(srcData, dataId, tgtData, reason, listener);
    }

    @Override
    public void getData(LogicalData ld, LogicalData tgtData, Transferable reason, EventListener listener) {
        getData(ld, ld.getName(), tgtData, reason, listener);
    }

    @Override
    public void getData(String dataId, String newName, String tgtDataId, Transferable reason, EventListener listener) {
        LogicalData srcData = Comm.getData(dataId);
        LogicalData tgtData = Comm.getData(tgtDataId);
        this.getData(srcData, newName, tgtData, reason, listener);
    }

    @Override
    public void getData(String dataId, String newName, LogicalData tgtData, Transferable reason,
            EventListener listener) {
        LogicalData ld = Comm.getData(dataId);
        this.getData(ld, newName, tgtData, reason, listener);
    }

    @Override
    public void getData(LogicalData ld, String newName, LogicalData tgtData, Transferable reason,
            EventListener listener) {
        if (reason.getType() == DataType.BINDING_OBJECT_T) {
            if (ld.getValue() == null) {
                LOGGER.warn("[Resource] Getting data: " + newName
                        + ", source logical data value is null. Trying with data target from reason ");
                BindingObject bo = BindingObject.generate((String) reason.getDataTarget());
                newName = newName + "#" + bo.getType() + "#" + bo.getElements();
            } else {
                BindingObject bo = BindingObject.generate((String) ld.getValue());
                newName = newName + "#" + bo.getType() + "#" + bo.getElements();
            }
        }
        SimpleURI workingPath = node.getCompletePath(reason.getType(), newName);
        DataLocation target = null;
        try {
            target = DataLocation.createLocation(this, workingPath);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + workingPath.toString(), e);
        }
        getData(ld, target, tgtData, reason, listener);
    }

    @Override
    public void getData(String dataId, DataLocation target, Transferable reason, EventListener listener) {
        LogicalData srcData = Comm.getData(dataId);
        getData(srcData, target, srcData, reason, listener);
    }

    @Override
    public void getData(String dataId, DataLocation target, String tgtDataId, Transferable reason,
            EventListener listener) {
        LogicalData srcData = Comm.getData(dataId);
        LogicalData tgtData = Comm.getData(tgtDataId);
        getData(srcData, target, tgtData, reason, listener);
    }

    @Override
    public void getData(String dataId, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {
        LogicalData ld = Comm.getData(dataId);
        getData(ld, target, tgtData, reason, listener);
    }

    @Override
    public void getData(LogicalData srcData, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {
        node.obtainData(srcData, null, target, tgtData, reason, listener);
    }

    @Override
    public void enforceDataObtaning(Transferable t, EventListener listener) {
        node.enforceDataObtaining(t, listener);
    }

    /**
     * Returns the complete remote path of a given data
     *
     * @param type
     * @param name
     * @return
     */
    @Override
    public SimpleURI getCompleteRemotePath(DataType type, String name) {
        return node.getCompletePath(type, name);
    }

    /**
     * Retrieves all the data from the Resource
     *
     * @param saveUniqueData
     */
    @Override
    public void retrieveData(boolean saveUniqueData) {
        if (DEBUG) {
            LOGGER.debug("Retrieving data resource " + this.getName());
        }
        Semaphore sem = new Semaphore(0);
        SafeCopyListener listener = new SafeCopyListener(sem);
        Set<LogicalData> lds = getAllDataFromHost();
        Map<String, String> disks = SharedDiskManager.terminate(this);
        COMPSsNode masterNode = Comm.getAppHost().getNode();
        for (LogicalData ld : lds) {
            if (ld.getCopiesInProgress().size() > 0) {
                ld.notifyToInProgressCopiesEnd(listener);
            }
            ld.lockHostRemoval();
            DataLocation lastLoc = ld.removeHostAndCheckLocationToSave(this, disks);
            ld.releaseHostRemoval();
            if (lastLoc != null && saveUniqueData) {
                listener.addOperation();

                DataLocation safeLoc = null;
                String safePath = null;
                boolean isBindingData = false;
                if (lastLoc.getType().equals(LocationType.BINDING)) {
                    BindingObject bo = BindingObject.generate(lastLoc.getPath());
                    safePath = ProtocolType.BINDING_URI.getSchema() + Comm.getAppHost().getTempDirPath() + ld.getName()
                            + "#" + bo.getType() + "#" + bo.getElements();
                } else {
                    safePath = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + ld.getName();
                }
                try {
                    SimpleURI uri = new SimpleURI(safePath);
                    safeLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + safePath, e);
                }

                if (isBindingData) {
                    masterNode.obtainData(ld, lastLoc, safeLoc, ld, new SafeCopyTransferable(DataType.BINDING_OBJECT_T),
                            listener);
                } else {
                    masterNode.obtainData(ld, lastLoc, safeLoc, ld, new SafeCopyTransferable(), listener);
                }
            }
        }

        if (DEBUG) {
            LOGGER.debug("Waiting for finishing saving copies for " + this.getName());
        }
        if (listener.getOperations() > 0) {
            listener.enable();
            try {
                sem.acquire();
            } catch (InterruptedException ex) {
                LOGGER.error("Error waiting for files in resource " + getName() + " to get saved");
            }
        }
        if (DEBUG) {
            LOGGER.debug("Unique files saved for " + this.getName());
        }

        if (this.getType() != ResourceType.SERVICE) {
            shutdownExecutionManager();

            if (Tracer.extraeEnabled() || Tracer.scorepEnabled() || Tracer.mapEnabled()) {
                if (node.generatePackage()) {
                    getTracingPackageToMaster();
                    if (DEBUG) {
                        LOGGER.debug("Tracing package obtained for " + this.getName());
                    }
                }
            }

            if (DEBUG) {
                if (node.generateWorkersDebugInfo()) {
                    getWorkersDebugInfo();
                    LOGGER.debug("Workers Debug files obtained for " + this.getName());
                }
            }
        }
    }

    @Override
    public void deleteIntermediate() {
        node.deleteTemporary();
    }

    @Override
    public void stop(ShutdownListener sl) {
        this.deleteIntermediate();
        sl.addOperation();
        node.stop(sl);
    }

    /**
     * Stops the Execution Manager of the resource.
     */
    private void shutdownExecutionManager() {
        if (DEBUG) {
            LOGGER.debug("Shutting down Execution Manager on Resource " + this.getName());
        }

        Semaphore sem = new Semaphore(0);
        ExecutorShutdownListener executorShutdownListener = new ExecutorShutdownListener(sem);

        executorShutdownListener.addOperation();
        node.shutdownExecutionManager(executorShutdownListener);
        executorShutdownListener.enable();
        if (DEBUG) {
            LOGGER.debug("Waiting for shutting down the execution manager of " + this.getName());
        }
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for execution manager in resource " + getName() + " to stop");
        }
        if (DEBUG) {
            LOGGER.debug("Execution manager of " + this.getName() + " stopped");
        }
    }

    private void getTracingPackageToMaster() {
        COMPSsNode masterNode = Comm.getAppHost().getNode();
        Semaphore sem = new Semaphore(0);
        String fileName = getName() + "_compss_trace.tar.gz";
        SimpleURI fileOriginURI = node.getCompletePath(DataType.FILE_T, fileName);

        if (DEBUG) {
            LOGGER.debug("Copying tracing package from : " + fileOriginURI.getPath() + ",to : "
                    + Comm.getAppHost().getAppLogDirPath() + "trace" + File.separator + fileName);
        }

        TracingCopyListener tracingListener = new TracingCopyListener(sem);

        tracingListener.addOperation();

        // Source data location
        DataLocation source;
        try {
            source = DataLocation.createLocation(this, fileOriginURI);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + fileOriginURI.getPath(), e);
            return;
        }

        // Target data location
        DataLocation tgt;
        String targetPath = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getAppLogDirPath() + "trace"
                + File.separator + fileName;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            tgt = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath);
            return;
        }

        // Ask for data
        masterNode.obtainData(new LogicalData("tracing" + this.getName()), source, tgt,
                new LogicalData("tracing" + this.getName()), new TracingCopyTransferable(), tracingListener);

        tracingListener.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for tracing files in resource " + getName() + " to get saved");
        }
        if (DEBUG) {
            LOGGER.debug("Removing " + this.getName() + " tracing temporary files");
        }

        File f = null;
        try {
            f = new File(source.getPath());
            if (!f.delete()) {
                LOGGER.error("Unable to remove tracing temporary files of node " + this.getName());
            }
        } catch (Exception e) {
            LOGGER.error("Unable to remove tracing temporary files of node " + this.getName(), e);
        }
    }

    private void getWorkersDebugInfo() {
        if (DEBUG) {
            LOGGER.debug("Copying Workers Information");
        }

        COMPSsNode masterNode = Comm.getAppHost().getNode();
        Semaphore sem = new Semaphore(0);
        WorkersDebugInformationListener wdil = new WorkersDebugInformationListener(sem);

        // Get Worker output
        wdil.addOperation();
        String outFileName = "worker_" + getName() + ".out";
        SimpleURI outFileOrigin = node.getCompletePath(DataType.FILE_T,
                "log" + File.separator + "static_" + outFileName);
        String outFileTarget = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkersDirPath()
                + File.separator + outFileName;

        DataLocation outSource = null;
        try {
            outSource = DataLocation.createLocation(this, outFileOrigin);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + outFileOrigin.toString(), e);
        }

        DataLocation outTarget = null;
        try {
            SimpleURI uri = new SimpleURI(outFileTarget);
            outTarget = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + outFileTarget);
        }

        LOGGER.debug("- Source: " + outFileOrigin);
        LOGGER.debug("- Target: " + outFileTarget);
        masterNode.obtainData(new LogicalData("workerOut" + this.getName()), outSource, outTarget,
                new LogicalData("workerOut" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

        // Get Worker error
        wdil.addOperation();
        String errFileName = "worker_" + getName() + ".err";
        SimpleURI errFileOrigin = node.getCompletePath(DataType.FILE_T,
                "log" + File.separator + "static_" + errFileName);
        String errFileTarget = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkersDirPath()
                + File.separator + errFileName;

        DataLocation errSource = null;
        try {
            errSource = DataLocation.createLocation(this, errFileOrigin);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + errFileOrigin.toString(), e);
        }

        DataLocation errTarget = null;
        try {
            SimpleURI uri = new SimpleURI(errFileTarget);
            errTarget = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + errFileTarget);
        }

        LOGGER.debug("- Source: " + errFileOrigin);
        LOGGER.debug("- Target: " + errFileTarget);
        masterNode.obtainData(new LogicalData("workerErr" + this.getName()), errSource, errTarget,
                new LogicalData("workerErr" + this.getName()), new WorkersDebugInfoCopyTransferable(), wdil);

        // Wait transfers
        wdil.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for worker debug files in resource " + getName() + " to get saved");
        }

        LOGGER.debug("Worker files from resource " + getName() + "received");
    }

}
