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
package es.bsc.compss.gat.worker.implementations;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.gat.worker.ImplementationDefinition;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.COMPSsImplementation;
import es.bsc.compss.types.implementations.MethodType;


public class COMPSsDefinition extends ImplementationDefinition {

    private final String runcompss;
    private final String flags;
    private final String appName;
    private final String workerInMaster;
    private final String workingDir;

    private final COMPSsImplementation impl;


    /**
     * Creates a new COMPSs definition implementation.
     * 
     * @param debug Whether the debug mode is enabled or not.
     * @param args Application arguments.
     * @param execArgsIdx Index of the start of the execution arguments.
     */
    public COMPSsDefinition(boolean debug, String[] args, int execArgsIdx) {
        super(debug, args, execArgsIdx + COMPSsImplementation.NUM_PARAMS);

        this.runcompss = args[execArgsIdx++];
        this.flags = args[execArgsIdx++];
        this.appName = args[execArgsIdx++];
        this.workerInMaster = args[execArgsIdx++];

        String wDir = args[execArgsIdx++];
        if (wDir == null || wDir.isEmpty() || wDir.equals(Constants.UNASSIGNED)) {
            this.workingDir = null;
        } else {
            this.workingDir = wDir;
        }

        this.impl = new COMPSsImplementation(this.runcompss, this.flags, this.appName, this.workerInMaster,
            this.workingDir, null, null, "", null);
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
    }

    @Override
    public MethodType getType() {
        return MethodType.COMPSs;
    }

    @Override
    public Lang getLang() {
        return null;
    }

    @Override
    public String toLogString() {
        return this.impl.getMethodDefinition();
    }

    @Override
    public long getTimeOut() {
        return 0L;
    }

}
