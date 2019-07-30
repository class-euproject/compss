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

import es.bsc.compss.connectors.ConnectorException;

import es.bsc.conn.Connector;
import es.bsc.conn.exceptions.ConnException;
import es.bsc.conn.types.HardwareDescription;
import es.bsc.conn.types.SoftwareDescription;
import es.bsc.conn.types.VirtualResource;

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
     * @return Machine Object.
     * @throws ConnectorException If an invalid connector is provided or if machine cannot be created.
     */
    public Object create(String name, HardwareDescription hardwareDescription, SoftwareDescription softwareDescription,
        Map<String, String> properties) throws ConnectorException {

        if (this.connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }
        Object created;
        try {
            created = this.connector.create(name, hardwareDescription, softwareDescription, properties);
        } catch (ConnException ce) {
            throw new ConnectorException(ce);
        }
        return created;
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
    public VirtualResource waitUntilCreation(Object id) throws ConnectorException {
        if (this.connector == null) {
            throw new ConnectorException(ERROR_NO_CONN);
        }

        VirtualResource vr;
        try {
            vr = this.connector.waitUntilCreation(id);
        } catch (ConnException ce) {
            throw new ConnectorException(ce);
        }
        return vr;
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
