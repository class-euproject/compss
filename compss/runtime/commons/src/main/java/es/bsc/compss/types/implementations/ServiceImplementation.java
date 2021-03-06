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

import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.ServiceResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class ServiceImplementation extends Implementation implements Externalizable {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 4;

    private String operation;


    /**
     * Creates a new ServiceImplementation for serialization.
     */
    public ServiceImplementation() {
        // For externalizable
        super();
    }

    /**
     * Creates a new ServiceImplementation instance from the given parameters.
     *
     * @param coreId Core Id.
     * @param namespace Service namespace.
     * @param service Service name.
     * @param port Service port.
     * @param operation Service operation.
     * @param signature Service operation signature.
     */
    public ServiceImplementation(Integer coreId, String namespace, String service, String port, String operation,
        String signature) {

        super(coreId, 0, signature, null);

        this.requirements = new ServiceResourceDescription(service, namespace, port, 1);
        this.operation = operation;
    }

    /**
     * Returns the service operation.
     *
     * @return
     */
    public String getOperation() {
        return this.operation;
    }

    /**
     * Builds a service signature from the given parameters.
     *
     * @param namespace Service namespace.
     * @param serviceName Service name.
     * @param portName Service port.
     * @param operation Service operation.
     * @param hasTarget Whether the service has a target object or not.
     * @param numReturns The number of return parameters of the service.
     * @param parameters The number of parameters of the method.
     * @return Signature built from the given parameters.
     */
    public static String getSignature(String namespace, String serviceName, String portName, String operation,
        boolean hasTarget, int numReturns, List<Parameter> parameters) {

        StringBuilder buffer = new StringBuilder();

        buffer.append(operation).append("(");
        int numPars = parameters.size();
        if (hasTarget) {
            numPars--;
        }

        numPars -= numReturns;
        if (numPars > 0) {
            buffer.append(parameters.get(0).getType());
            for (int i = 1; i < numPars; i++) {
                buffer.append(",").append(parameters.get(i).getType());
            }
        }
        buffer.append(")").append(namespace).append(',').append(serviceName).append(',').append(portName);

        return buffer.toString();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.SERVICE;
    }

    @Override
    public ServiceResourceDescription getRequirements() {
        return (ServiceResourceDescription) this.requirements;
    }

    @Override
    public String toString() {
        ServiceResourceDescription description = (ServiceResourceDescription) this.requirements;
        return super.toString() + " Service in namespace " + description.getNamespace() + " with name "
            + description.getPort() + " on port " + description.getPort() + "and operation " + this.operation;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.operation = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.operation);
    }

}
