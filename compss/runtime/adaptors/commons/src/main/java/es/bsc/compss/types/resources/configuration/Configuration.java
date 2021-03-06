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
package es.bsc.compss.types.resources.configuration;

import es.bsc.compss.log.Loggers;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Basic worker resource configuration.
 */
public class Configuration {

    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);

    private final String adaptorName;
    private final HashMap<String, String> additionalProperties = new HashMap<>();
    private int limitOfTasks = -1;
    private int limitOfGPUTasks = -1;
    private int limitOfFPGATasks = -1;
    private int limitOfOTHERsTasks = -1;


    /**
     * Instantiates the class.
     *
     * @param adaptorName Adaptor name
     */
    public Configuration(String adaptorName) {
        this.adaptorName = adaptorName;
    }

    /**
     * Clones a class instance.
     *
     * @param clone Configuration to clone
     */
    public Configuration(Configuration clone) {
        this.adaptorName = clone.adaptorName;

        this.limitOfTasks = clone.limitOfTasks;
        this.limitOfGPUTasks = clone.limitOfGPUTasks;
        this.limitOfFPGATasks = clone.limitOfFPGATasks;
        this.limitOfOTHERsTasks = clone.limitOfOTHERsTasks;

        for (Entry<String, String> addProp : clone.additionalProperties.entrySet()) {
            additionalProperties.put(addProp.getKey(), addProp.getValue());
        }
    }

    /**
     * Returns the adaptor name (fully qualified classname).
     *
     * @return
     */
    public final String getAdaptorName() {
        return this.adaptorName;
    }

    /**
     * Returns a HashMap with all the additional properties. The keys are the names of the properties and the values are
     * the values of the properties
     *
     * @return
     */
    public final HashMap<String, String> getAdditionalProperties() {
        return this.additionalProperties;
    }

    /**
     * Returns the value of the property with name @name. Null if key doesn't exist
     *
     * @param name Property name.
     * @return
     */
    public final String getProperty(String name) {
        return this.additionalProperties.get(name);
    }

    /**
     * Adds a property with name @name and value @value.
     *
     * @param name Property name
     * @param value Property value
     */
    public final void addProperty(String name, String value) {
        this.additionalProperties.put(name, value);
    }

    /**
     * Gets the limit of tasks.
     *
     * @return
     */
    public int getLimitOfTasks() {
        return limitOfTasks;
    }

    /**
     * Sets the limit of tasks.
     *
     * @param limitOfTasks maximum number of tasks
     */
    public void setLimitOfTasks(int limitOfTasks) {
        this.limitOfTasks = limitOfTasks;
    }

    /**
     * Gets the limit of GPU tasks.
     *
     * @return
     */
    public int getLimitOfGPUTasks() {
        return limitOfGPUTasks;
    }

    /**
     * Sets the limit of GPU tasks.
     *
     * @param limitOfGPUTasks Limit of tasks with GPUS
     */
    public void setLimitOfGPUTasks(int limitOfGPUTasks) {
        this.limitOfGPUTasks = limitOfGPUTasks;
    }

    /**
     * Gets the limit of FPGA tasks.
     *
     * @return
     */
    public int getLimitOfFPGATasks() {
        return limitOfFPGATasks;
    }

    /**
     * Sets the limit of FPGA tasks.
     *
     * @param limitOfFPGATasks Limit of tasks with GPU
     */
    public void setLimitOfFPGATasks(int limitOfFPGATasks) {
        this.limitOfFPGATasks = limitOfFPGATasks;
    }

    /**
     * Gets the limit of OTHER tasks.
     *
     * @return
     */
    public int getLimitOfOTHERsTasks() {
        return limitOfOTHERsTasks;
    }

    /**
     * Sets the limit of OTHERS tasks.
     *
     * @param limitOfOTHERsTasks Limit of task with OTHER devices
     */
    public void setLimitOfOTHERsTasks(int limitOfOTHERsTasks) {
        this.limitOfOTHERsTasks = limitOfOTHERsTasks;
    }

}
