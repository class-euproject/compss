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
import es.bsc.compss.types.implementations.DecafImplementation;
import es.bsc.compss.types.implementations.MethodType;


public class DecafDefinition extends ImplementationDefinition {

    private final String dfScript;
    private final String dfExecutor;
    private final String dfLib;
    private final String mpiRunner;
    private final String workingDir;

    private final DecafImplementation impl;


    /**
     * Creates a new Decaf definition implementation.
     * 
     * @param debug Whether the debug mode is enabled or not.
     * @param args Application arguments.
     * @param execArgsIdx Index of the start of the execution arguments.
     */
    public DecafDefinition(boolean debug, String[] args, int execArgsIdx) {
        super(debug, args, execArgsIdx + DecafImplementation.NUM_PARAMS);
        
        this.dfScript = args[execArgsIdx++];
        this.dfExecutor = args[execArgsIdx++];
        this.dfLib = args[execArgsIdx++];
        this.mpiRunner = args[execArgsIdx++];

        String wDir = args[execArgsIdx];
        if ((wDir == null || wDir.isEmpty() || wDir.equals(Constants.UNASSIGNED))) {
            this.workingDir = null;
        } else {
            this.workingDir = wDir;
        }

        this.impl = new DecafImplementation(this.dfScript, this.dfExecutor, this.dfLib, this.workingDir, this.mpiRunner,
                null, null, null);
    }

    @Override
    public AbstractMethodImplementation getMethodImplementation() {
        return this.impl;
    }

    @Override
    public MethodType getType() {
        return MethodType.DECAF;
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
    public int getTimeOut() {
        return 0;
    }

}
