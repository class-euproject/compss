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
package es.bsc.compss.types.data;

import java.util.LinkedList;
import java.util.TreeMap;

import es.bsc.compss.comm.Comm;


// Information about a datum and its versions
public abstract class DataInfo {

    private static final int FIRST_FILE_ID = 1;
    private static final int FIRST_VERSION_ID = 1;

    protected static int nextDataId = FIRST_FILE_ID;
    // Data identifier
    protected int dataId;

    // Current version
    protected DataVersion currentVersion;
    // Data and version identifier management
    protected int currentVersionId;

    // Versions of the datum
    // Map: version identifier -> version
    protected TreeMap<Integer, DataVersion> versions;
    // private boolean toDelete;

    protected int deletionBlocks;
    protected final LinkedList<DataVersion> pendingDeletions;
    protected final  TreeMap<Integer, Boolean> canceledVersions;
    
    protected Boolean canceled;
    public DataInfo() {
        this.dataId = nextDataId++;
        this.versions = new TreeMap<>();
        this.currentVersionId = FIRST_VERSION_ID;
        this.currentVersion = new DataVersion(dataId, 1);
        Comm.registerData(currentVersion.getDataInstanceId().getRenaming());
        this.versions.put(currentVersionId, currentVersion);
        this.deletionBlocks = 0;
        this.pendingDeletions = new LinkedList<>();
        this.canceledVersions = new TreeMap<>();
        this.canceled = false;
    }

    public int getDataId() {
        return dataId;
    }

    public int getCurrentVersionId() {
        return currentVersionId;
    }

    /*
     * public DataInstanceId getCurrentDataInstanceId() { return currentVersion.getDataInstanceId(); }
     * 
     * public DataInstanceId getPreviousDataInstanceId() { DataVersion dv =versions.get(currentVersionId-1); if
     * (dv!=null){ return dv.getDataInstanceId(); }else{ return null; } }
     */

    public DataVersion getCurrentDataVersion() {
        return currentVersion;
    }

    public DataVersion getPreviousDataVersion() {
        return versions.get(currentVersionId - 1);
    }

    public void willBeRead() {
        currentVersion.willBeRead();
        canceledVersions.put(currentVersionId, false);
    }

    public boolean isToBeRead() {
        return currentVersion.hasPendingLectures();
    }

    public boolean versionHasBeenRead(int versionId) {
        DataVersion readVersion = versions.get(versionId);
        if (readVersion.hasBeenRead()) {
            Comm.removeData(readVersion.getDataInstanceId().getRenaming());
            versions.remove(versionId);
            // return (this.toDelete && versions.size() == 0);
            return versions.isEmpty();
        }
        return false;
    }

    public void willBeWritten() {
        currentVersionId++;
        DataVersion newVersion = new DataVersion(dataId, currentVersionId);
        Comm.registerData(newVersion.getDataInstanceId().getRenaming());
        newVersion.willBeWritten();
        versions.put(currentVersionId, newVersion);
        canceledVersions.put(currentVersionId, false);
        currentVersion = newVersion;
    }

    public boolean versionHasBeenWritten(int versionId) {
        DataVersion writtenVersion = versions.get(versionId);
        if (writtenVersion.hasBeenWritten()) {
            Comm.removeData(writtenVersion.getDataInstanceId().getRenaming());
            versions.remove(versionId);
            // return (this.toDelete && versions.size() == 0);
            return versions.isEmpty();
        }
        return false;
    }

    public void blockDeletions() {
        deletionBlocks++;
    }

    public boolean unblockDeletions() {
        deletionBlocks--;
        if (deletionBlocks == 0) {
            for (DataVersion version : pendingDeletions) {
                if (version.markToDelete()) {
                    Comm.removeData(version.getDataInstanceId().getRenaming());
                    versions.remove(version.getDataInstanceId().getVersionId());
                }
            }
            if (versions.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public boolean delete() {
        if (deletionBlocks > 0) {
            pendingDeletions.addAll(versions.values());
        } else {
            LinkedList<Integer> removedVersions = new LinkedList<>();
            for (DataVersion version : versions.values()) {
                if (version.markToDelete()) {
                    Comm.removeData(version.getDataInstanceId().getRenaming());
                    removedVersions.add(version.getDataInstanceId().getVersionId());
                }
            }
            for (int versionId : removedVersions) {
                versions.remove(versionId);
            }
            if (versions.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public boolean isCurrentVersionToDelete(){
        if (canceled) return getLastVersion(this.currentVersionId).isToDelete();
    	return currentVersion.isToDelete();
    }

    public DataVersion getFirstVersion() {
        return versions.get(1);
    }
    
    public DataVersion getLastVersion(Integer versionId){
        Boolean canceled = true;
        while (canceled == true && versionId > 0) {
            canceled = canceledVersions.get(versionId);
            if (canceled) {
                tryRemoveVersion(versionId);
                canceledVersions.remove(versionId);
                versionId = versionId - 1;
            }
        }
        
        currentVersionId = versionId;
        currentVersion = versions.get(currentVersionId);
        return versions.get(versionId);
    }

    public void tryRemoveVersion(Integer versionId) {
        DataVersion readVersion = versions.get(versionId);

        if (readVersion != null && readVersion.markToDelete()) {
            Comm.removeData(readVersion.getDataInstanceId().getRenaming());
            versions.remove(versionId);
        }

    }
    public void canceledVersion(Integer versionId) {
        canceledVersions.put(versionId, true);
        canceled = true;
    }
}
