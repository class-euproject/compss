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

import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class OpenCLImplementation extends AbstractMethodImplementation implements Externalizable {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 2;

    private String kernel;
    private String workingDir;


    /**
     * Creates a new OpenCLImplementation for serialization.
     */
    public OpenCLImplementation() {
        // For externalizable
        super();
    }

    /**
     * Creates a new OpenCLImplementation instance from the given parameters.
     * 
     * @param kernel Path to the OpenCL kernel.
     * @param workingDir Binary working directory.
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param signature Method signature.
     * @param annot Method requirements.
     */
    public OpenCLImplementation(String kernel, String workingDir, Integer coreId, Integer implementationId,
        String signature, MethodResourceDescription annot) {

        super(coreId, implementationId, signature, annot);

        this.kernel = kernel;
        this.workingDir = workingDir;
    }

    /**
     * Returns the path to the OpenCL kernel.
     * 
     * @return The path to the OpenCL kernel.
     */
    public String getKernel() {
        return this.kernel;
    }

    /**
     * Returns the binary working directory.
     * 
     * @return The binary working directory.
     */
    public String getWorkingDir() {
        return this.workingDir;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.OPENCL;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[KERNEL=").append(this.kernel);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " OpenCL Method with kernel " + this.kernel;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.kernel = (String) in.readObject();
        this.workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.kernel);
        out.writeObject(this.workingDir);
    }

}
