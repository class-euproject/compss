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
package es.bsc.compss.types.implementations;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.compss.types.resources.MethodResourceDescription;


public class DecafImplementation extends AbstractMethodImplementation implements Externalizable {

    public static final int NUM_PARAMS = 5;

    public static final String SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts" + File.separator
            + "system" + File.separator + "decaf" + File.separator + "run_decaf.sh";

    private String mpiRunner;
    private String dfScript;
    private String dfExecutor;
    private String dfLib;
    private String workingDir;


    public DecafImplementation() {
        // For externalizable
        super();
    }

    public DecafImplementation(String dfScript, String dfExecutor, String dfLib, String workingDir, String mpiRunner,
            Integer coreId, Integer implementationId, MethodResourceDescription annot) {

        super(coreId, implementationId, annot);

        this.mpiRunner = mpiRunner;
        this.workingDir = workingDir;
        this.dfScript = dfScript;
        this.dfExecutor = dfExecutor;
        this.dfLib = dfLib;
    }

    public String getDfScript() {
        return this.dfScript;
    }

    public String getDfExecutor() {
        return this.dfExecutor;
    }

    public String getDfLib() {
        return this.dfLib;
    }

    public String getWorkingDir() {
        return this.workingDir;
    }

    public String getMpiRunner() {
        return this.mpiRunner;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.DECAF;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[MPI RUNNER=").append(this.mpiRunner);
        sb.append(", DF_SCRIPT=").append(this.dfScript);
        sb.append(", DF_EXECUTOR=").append(this.dfExecutor);
        sb.append(", DF_LIBRARY=").append(this.dfLib);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " Decaf Method with script " + this.dfScript + ", executor " + this.dfScript
                + ", library " + this.dfLib + " and MPIrunner " + this.mpiRunner;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.mpiRunner = (String) in.readObject();
        this.dfScript = (String) in.readObject();
        this.dfExecutor = (String) in.readObject();
        this.dfLib = (String) in.readObject();
        this.workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.mpiRunner);
        out.writeObject(this.dfScript);
        out.writeObject(this.dfExecutor);
        out.writeObject(this.dfLib);
        out.writeObject(this.workingDir);
    }

}
