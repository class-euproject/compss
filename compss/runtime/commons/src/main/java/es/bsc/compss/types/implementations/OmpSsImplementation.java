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


public class OmpSsImplementation extends AbstractMethodImplementation implements Externalizable {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 2;

    private String binary;
    private String workingDir;


    /**
     * Creates a new OmpSsImplementation for serialization.
     */
    public OmpSsImplementation() {
        // For externalizable
        super();
    }

    /**
     * Creates a new OmpSsImplementation instance from the given parameters.
     * 
     * @param binary Path to the OmpSs binary.
     * @param workingDir Binary working directory.
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param annot Method annotations.
     */
    public OmpSsImplementation(String binary, String workingDir, Integer coreId, Integer implementationId,
            MethodResourceDescription annot) {
        super(coreId, implementationId, annot);

        this.binary = binary;
        this.workingDir = workingDir;
    }

    /**
     * Returns the path to the OmpSs binary.
     * 
     * @return The path to the OmpSs binary.
     */
    public String getBinary() {
        return this.binary;
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
        return MethodType.OMPSS;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[BINARY=").append(this.binary);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " OmpSs Method with binary " + this.binary;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.binary = (String) in.readObject();
        this.workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.binary);
        out.writeObject(this.workingDir);
    }

}
