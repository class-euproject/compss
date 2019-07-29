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
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.types.COMPSsMaster;
import es.bsc.compss.types.uri.MultiURI;

import java.util.HashMap;
import java.util.Map;


public class MasterResourceImpl extends DynamicMethodWorker implements MasterResource {

    /**
     * Creates a new Master Resource.
     */
    public MasterResourceImpl() {
        super(COMPSsMaster.getMasterName(), // Master name
            new MethodResourceDescription(), // Master resource description
            new COMPSsMaster(), // Master COMPSs node
            0, // limit of tasks
            0, // Limit of GPU tasks
            0, // Limit of FPGA tasks
            0, // Limit of OTHER tasks
            new HashMap<>()// Shared disks
        );
    }

    /**
     * Returns the COMPSs base log directory.
     *
     * @return The COMPSs base log directory.
     */
    public String getCOMPSsLogBaseDirPath() {
        return ((COMPSsMaster) this.getNode()).getCOMPSsLogBaseDirPath();
    }

    @Override
    public String getWorkingDirectory() {
        return ((COMPSsMaster) this.getNode()).getWorkingDirectory();
    }

    public String getUserExecutionDirPath() {
        return ((COMPSsMaster) this.getNode()).getUserExecutionDirPath();
    }

    @Override
    public String getAppLogDirPath() {
        return ((COMPSsMaster) this.getNode()).getAppLogDirPath();
    }

    @Override
    public String getTempDirPath() {
        return ((COMPSsMaster) this.getNode()).getTempDirPath();
    }

    @Override
    public String getJobsDirPath() {
        return ((COMPSsMaster) this.getNode()).getJobsDirPath();
    }

    @Override
    public String getWorkersDirPath() {
        return ((COMPSsMaster) this.getNode()).getWorkersDirPath();
    }

    @Override
    public void setInternalURI(MultiURI u) {
        for (CommAdaptor adaptor : Comm.getAdaptors().values()) {
            adaptor.completeMasterURI(u);
        }
    }

    @Override
    public ResourceType getType() {
        return ResourceType.MASTER;
    }

    @Override
    public void retrieveUniqueDataValues() {
        // All data is kept within the master process. No need to bring to the node any data since it is already there.
    }

    @Override
    public int compareTo(Resource t) {
        if (t.getType() == ResourceType.MASTER) {
            return getName().compareTo(t.getName());
        } else {
            return 1;
        }
    }

    @Override
    public void updateDisks(Map<String, String> sharedDisks) {
        super.sharedDisks = sharedDisks;
    }

}
