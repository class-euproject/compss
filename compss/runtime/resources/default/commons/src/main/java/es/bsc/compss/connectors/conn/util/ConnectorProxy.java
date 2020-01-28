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
package es.bsc.compss.connectors.conn.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.connectors.ConnectorException;

import es.bsc.conn.Connector;
import es.bsc.conn.exceptions.ConnException;
import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.StarterCommand;
import es.bsc.conn.types.VirtualResource;

import java.util.List;
import java.util.Map;


public class ConnectorProxy {

    // Constraints default values
    private static final String ERROR_NO_CONN = "ERROR: Connector specific implementation is null";

    private final Connector connector;


    /**
     * Creates a new ConnectorProxy instance with an associated Connector.
     * 
     * @param conn Associated connector.
     * @throws ConnectorException If an invalid connector is provided.
     */
    public ConnectorProxy(Connector conn) throws ConnectorException {
        if (conn == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
        this.connector = conn;
    }

    /**
     * Creates a new machine in the given connector with the given information.
     * 
     * @param name Machine name.
     * @param hardwareDescription Connector hardware properties.
     * @param softwareDescription Connector software properties.
     * @param properties Specific properties.
     * @param starterCMD Starter command
     * @param replicas Amount of replicas to deploy
     * @return Machine Object.
     * @throws ConnectorException If an invalid connector is provided or if machine cannot be created.
     */
    public Object create(String name, HardwareDescription hardwareDescription, SoftwareDescription softwareDescription,
        Map<String, String> properties, StarterCommand starterCMD, int replicas) throws ConnectorException {
        System.out.println("ConnectorProxy::create");
        if (this.connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
        Object created;
        try {
            created =
                this.connector.create(name, hardwareDescription, softwareDescription, properties, starterCMD, replicas);
        } catch (ConnException ce) {
            throw new ConnectorException(ce);
        }
        return created;
    }

    private StarterCommand getStarterCommand(String adaptorName, String name, HardwareDescription hd,
        SoftwareDescription sd, Map<String, String> properties, boolean container) {
        String workerName = name;
        int workerPort = -1; //
        String masterName = System.getProperty(COMPSsConstants.MASTER_NAME); //
        String workingDir = null; //
        String installDir = null; //
        String appDir = null; //
        String classpathFromFile = null; //
        String pythonpathFromFile = null; //
        String libPathFromFile = null; //
        int totalCPU = hd.getTotalCPUComputingUnits(); //
        int totalGPU = hd.getTotalGPUComputingUnits(); //
        int totalFPGA = hd.getTotalFPGAComputingUnits(); //
        int limitOfTasks = hd.getTotalCPUComputingUnits(); //
        String hostId = "NoTracingHostID"; //
        return Comm.getStarterCommand(adaptorName, workerName, workerPort, masterName, workingDir, installDir, appDir,
            classpathFromFile, pythonpathFromFile, libPathFromFile, totalCPU, totalGPU, totalFPGA, limitOfTasks, hostId,
            container);
    }

    /**
     * Destroys a given machine in the connector.
     * 
     * @param id Machine Id.
     * @throws ConnectorException If an invalid connector is set.
     */
    public void destroy(Object id) throws ConnectorException {
        if (this.connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }

        this.connector.destroy(id);
    }

    /**
     * Stops the current thread until the machine has been created.
     * 
     * @param id Machine Id.
     * @return Virtual Resoure representing the machine.
     * @throws ConnectorException If an invalid connector is set.
     */
    public List<VirtualResource> waitUntilCreation(Object... id) throws ConnectorException {
        System.out.println("ConnectorProxy::waitUntilCreation");
        if (this.connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }

        List<VirtualResource> vrs;
        try {
            vrs = this.connector.waitUntilCreation(id);
        } catch (ConnException ce) {
            throw new ConnectorException(ce);
        }
        return vrs;
    }

    /**
     * Returns the price slot of the given virtual resource.
     * 
     * @param vr Virtual Resource.
     * @param defaultPrice Default slot price.
     * @return The virtual resource price slot.
     */
    public float getPriceSlot(VirtualResource vr, float defaultPrice) {
        if (this.connector == null) {
            return defaultPrice;
        }

        return this.connector.getPriceSlot(vr);
    }

    /**
     * Returns the time slot of the connector.
     * 
     * @param defaultLength Default time slot.
     * @return The time slot of the connector.
     */
    public long getTimeSlot(long defaultLength) {
        if (this.connector == null) {
            return defaultLength;
        }
        return this.connector.getTimeSlot();
    }

    /**
     * Closes the associated connector.
     */
    public void close() {
        this.connector.close();
    }

}
