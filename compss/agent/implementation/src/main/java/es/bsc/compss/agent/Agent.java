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
package es.bsc.compss.agent;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;

import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.ImplementationDefinition;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.parsers.ITFParser;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import storage.StorageException;
import storage.StorageItf;


public class Agent {

    // private static final String AGENT_NAME = System.getProperty(AgentConstants.COMPSS_AGENT_NAME);
    private static final COMPSsRuntimeImpl RUNTIME;

    private static final Random APP_ID_GENERATOR = new Random();
    private static final List<AgentInterface<?>> INTERFACES;

    static {
        String dcConfigPath = System.getProperty(AgentConstants.DATACLAY_CONFIG_PATH);
        System.out.println("DataClay configuration: " + dcConfigPath);
        if (dcConfigPath != null) {
            try {
                StorageItf.init(dcConfigPath);
            } catch (StorageException se) {
                se.printStackTrace(System.err);
                System.err.println("Continuing...");
            }
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        StorageItf.finish();
                    } catch (StorageException se) {
                        se.printStackTrace(System.err);
                        System.err.println("Continuing...");
                    }
                }
            });
        }

        RUNTIME = new COMPSsRuntimeImpl();
        RUNTIME.setObjectRegistry(new ObjectRegistry(RUNTIME));
        RUNTIME.setStreamRegistry(new StreamRegistry(RUNTIME));
        RUNTIME.startIT();

        CoreElementDefinition ced = new CoreElementDefinition();
        ced.setCeSignature("load(OBJECT_T,OBJECT_T,STRING_T,LONG_T,STRING_T,STRING_T,OBJECT_T)");
        MethodResourceDescription mrd = new MethodResourceDescription("");
        for (Processor p : mrd.getProcessors()) {
            p.setName("LocalProcessor");
        }
        ImplementationDefinition implDef = ImplementationDefinition.defineImplementation("METHOD",
                "load(OBJECT_T,OBJECT_T,STRING_T,LONG_T,STRING_T,STRING_T,OBJECT_T)es.bsc.compss.agent.loader.Loader",
                new MethodResourceDescription(""), "es.bsc.compss.agent.loader.Loader", "load");
        ced.addImplementation(implDef);
        RUNTIME.registerCoreElement(ced);

        String hostName = System.getProperty(AgentConstants.COMPSS_AGENT_NAME);
        if (hostName == null) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                hostName = "localhost";
            }
        }
        System.setProperty(AgentConstants.COMPSS_AGENT_NAME, hostName);

        INTERFACES = new LinkedList<>();
    }


    /**
     * Request the execution of a method tasks and detect possible nested tasks.
     *
     * @param lang programming language of the method
     * @param ceiClass Core Element interface to detect nested tasks in the code
     * @param className name of the class containing the method to execute
     * @param methodName name of the method to execute
     * @param params parameter values to pass in to the method
     * @param monitor monitor to notify changes on the method execution
     * @return Identifier of the application associated to the main task
     * @throws AgentException error parsing the CEI
     */
    public static long runMain(Lang lang, String ceiClass, String className, String methodName, Object[] params,
            AppMonitor monitor) throws AgentException {

        long appId = Math.abs(APP_ID_GENERATOR.nextLong());
        long mainAppId = Math.abs(APP_ID_GENERATOR.nextLong());
        monitor.setAppId(mainAppId);

        try {
            Class<?> cei = Class.forName(ceiClass);
            List<CoreElementDefinition> ceds = ITFParser.parseITFMethods(cei);
            for (CoreElementDefinition ced : ceds) {
                RUNTIME.registerCoreElement(ced);
            }
        } catch (ClassNotFoundException cnfe) {
            throw new AgentException("Could not find class " + ceiClass + " to detect internal methods.");
        }

        Object[] paramsValues = new Object[] { RUNTIME, DataType.OBJECT_T, Direction.IN, StdIOStream.UNSPECIFIED, "",
                "runtime", // Runtime API
                RUNTIME, DataType.OBJECT_T, Direction.IN, StdIOStream.UNSPECIFIED, "", "api", // Loader API
                ceiClass, DataType.STRING_T, Direction.IN, StdIOStream.UNSPECIFIED, "", "ceiClass", // CEI
                appId, DataType.LONG_T, Direction.IN, StdIOStream.UNSPECIFIED, "", "appId", // Nested tasks App ID
                className, DataType.STRING_T, Direction.IN, StdIOStream.UNSPECIFIED, "", "className", // Class name
                methodName, DataType.STRING_T, Direction.IN, StdIOStream.UNSPECIFIED, "", "methodName", // Method name
                params, DataType.OBJECT_T, Direction.IN, StdIOStream.UNSPECIFIED, "", "params", // Method arguments
                new Object(), DataType.OBJECT_T, Direction.OUT, StdIOStream.UNSPECIFIED, "", "return" // Return value
        };

        RUNTIME.executeTask(mainAppId, // Task application ID
                monitor, // Corresponding task monitor
                lang, "es.bsc.compss.agent.loader.Loader", "load", // Method to run
                false, 1, false, false, // Scheduler hints
                false, 8, // Parameters information
                OnFailure.RETRY, // On failure behavior
                paramsValues // Argument values
        );
        return mainAppId;
    }

    /**
     * Requests the execution of a method as a task.
     *
     * @param lang programming language of the method
     * @param className name of the class containing the method to execute
     * @param methodName name of the method to execute
     * @param sarParams paramter description of the task
     * @param target paramter description of the task callee
     * @param hasResult true if the task returns any value
     * @param monitor monitor to notify changes on the method execution
     * @return Identifier of the application associated to the task
     * @throws AgentException could not retrieve the value of some parameter
     */
    public static long runTask(Lang lang, String className, String methodName, ApplicationParameter[] sarParams,
            ApplicationParameter target, boolean hasResult, AppMonitor monitor) throws AgentException {
        long appId = Math.abs(APP_ID_GENERATOR.nextLong());
        monitor.setAppId(appId);
        try {
            // PREPARING PARAMETERS
            StringBuilder typesSB = new StringBuilder();

            int paramsCount = sarParams.length;
            if (target != null) {
                paramsCount++;
            }
            if (hasResult) {
                paramsCount++;
            }

            Object[] params = new Object[6 * paramsCount];
            int position = 0;
            for (ApplicationParameter param : sarParams) {
                if (typesSB.length() > 0) {
                    typesSB.append(",");
                }
                if (param.getType() != DataType.PSCO_T) {
                    typesSB.append(param.getType().toString());
                } else {
                    typesSB.append("OBJECT_T");
                }
                params[position] = param.getValueContent();
                params[position + 1] = param.getType();
                params[position + 2] = param.getDirection();
                params[position + 3] = StdIOStream.UNSPECIFIED;
                params[position + 4] = ""; // Prefix
                params[position + 5] = ""; // Parameter Name
                position += 6;
            }

            if (target != null) {
                params[position] = target.getValueContent();
                params[position + 1] = target.getType();
                params[position + 2] = target.getDirection();
                params[position + 3] = StdIOStream.UNSPECIFIED;
                params[position + 4] = "";
                params[position + 5] = ""; // Parameter Name
                position += 6;
            }

            if (hasResult) {
                params[position] = null;
                params[position + 1] = DataType.OBJECT_T;
                params[position + 2] = Direction.OUT;
                params[position + 3] = StdIOStream.UNSPECIFIED;
                params[position + 4] = "";
                params[position + 5] = "";
                position += 6;
            }

            String paramsTypes = typesSB.toString();

            String ceSignature = methodName + "(" + paramsTypes + ")";
            String implSignature = methodName + "(" + paramsTypes + ")" + className;
            CoreElementDefinition ced = new CoreElementDefinition();
            ced.setCeSignature(ceSignature);
            ImplementationDefinition implDef = ImplementationDefinition.defineImplementation("METHOD", implSignature,
                    new MethodResourceDescription(""), className, methodName);
            ced.addImplementation(implDef);
            RUNTIME.registerCoreElement(ced);

            RUNTIME.executeTask(appId, // APP ID
                    monitor, // Corresponding task monitor
                    lang, className, methodName, // Method to call
                    false, 1, false, false, // Scheduling information
                    target != null, paramsCount, // Parameter information
                    OnFailure.RETRY, // On failure behavior
                    params // Parameter values
            );

        } catch (Exception e) {
            throw new AgentException(e);
        }
        return appId;
    }

    /**
     * Adds new resources into the resource pool.
     *
     * @param r Description of the resources to add into the resource pool
     * @throws AgentException could not create a configuration to start using this resource
     */
    public static void addResources(Resource<?, ?> r) throws AgentException {
        String workerName = r.getName();
        String adaptor = r.getAdaptor();
        MethodResourceDescription description = r.getDescription();
        Object projectConf = r.getProjectConf();
        Object resourcesConf = r.getResourceConf();

        DynamicMethodWorker worker = ResourceManager.getDynamicResource(workerName);
        if (worker != null) {
            ResourceManager.increasedDynamicWorker(worker, description);
        } else {
            MethodConfiguration mc;
            try {
                mc = (MethodConfiguration) Comm.constructConfiguration(adaptor, projectConf, resourcesConf);
            } catch (ConstructConfigurationException e) {
                throw new AgentException(e.getMessage(), e);
            }
            int limitOfTasks = mc.getLimitOfTasks();
            int computingUnits = description.getTotalCPUComputingUnits();
            if (limitOfTasks < 0 && computingUnits < 0) {
                mc.setLimitOfTasks(0);
                mc.setTotalComputingUnits(0);
            } else {
                mc.setLimitOfTasks(Math.max(limitOfTasks, computingUnits));
                mc.setTotalComputingUnits(Math.max(limitOfTasks, computingUnits));
            }
            mc.setLimitOfGPUTasks(description.getTotalGPUComputingUnits());
            mc.setTotalGPUComputingUnits(description.getTotalGPUComputingUnits());
            mc.setLimitOfFPGATasks(description.getTotalFPGAComputingUnits());
            mc.setTotalFPGAComputingUnits(description.getTotalFPGAComputingUnits());
            mc.setLimitOfOTHERsTasks(description.getTotalOTHERComputingUnits());
            mc.setTotalOTHERComputingUnits(description.getTotalOTHERComputingUnits());

            mc.setHost(workerName);
            DynamicMethodWorker mw = new DynamicMethodWorker(workerName, description, mc, new HashMap<>());
            ResourceManager.addDynamicWorker(mw, description);
        }
    }

    /**
     * Requests the agent to stop using some resources from a node.
     *
     * @param workerName name of the worker to whom the resources belong.
     * @param reduction description of the resources to stop using.
     * @throws AgentException the worker was not set up for the agent.
     */
    public static void removeResources(String workerName, MethodResourceDescription reduction) throws AgentException {
        DynamicMethodWorker worker = ResourceManager.getDynamicResource(workerName);
        if (worker != null) {
            ResourceManager.requestWorkerReduction(worker, reduction);
        } else {
            throw new AgentException("Resource " + workerName + " was not set up for this agent. Ignoring request.");
        }
    }

    /**
     * Request the agent to stop using all the resources from a node.
     *
     * @param workerName name of the worker to stop using
     * @throws AgentException the worker was not set up for the agent.
     */
    public static void removeNode(String workerName) throws AgentException {
        try {
            ResourceManager.requestWholeWorkerReduction(workerName);
        } catch (NullPointerException e) {
            throw new AgentException("Resource " + workerName + " was not set up for this agent. Ignoring request.");
        }
    }

    /**
     * Forces the agent to remove a node with which it has lost the connection.
     *
     * @param workerName name of the worker to stop using
     * @throws AgentException the worker was not set up for the agent.
     */
    public static void lostNode(String workerName) throws AgentException {
        try {
            ResourceManager.notifyWholeWorkerReduction(workerName);
        } catch (NullPointerException e) {
            throw new AgentException("Resource " + workerName + " was not set up for this agent. Ignoring request.");
        }
    }

    /**
     * Starts an agent interface.
     *
     * @param conf Agent Interface configuration parameters
     * @throws ClassNotFoundException Could not find the specify agent interface class
     * @throws InstantiationException Could not instantiate the agent interface
     * @throws IllegalAccessException Could not call the empty constructor because is private
     * @throws AgentException Error during the interface boot process
     */
    public static final void startInterface(AgentInterfaceConfig conf)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, AgentException {

        Class<?> agentClass = Class.forName(conf.getInterfaceClass());
        AgentInterface itf = (AgentInterface) agentClass.newInstance();
        itf.start(conf);
        INTERFACES.add(itf);
    }

    private static AgentInterfaceConfig getConfig(String className, String arguments)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, AgentException {

        Class<?> agentClass = Class.forName(className);
        AgentInterface<?> itf = (AgentInterface<?>) agentClass.newInstance();
        return itf.configure(arguments);
    }

    /**
     * Entry point.
     * 
     * @param args Command line arguments.
     * @throws Exception Any internal exception.
     */
    public static final void main(String[] args) throws Exception {
        // TODO: Read Agents Setup
        LinkedList<AgentInterfaceConfig> agents = new LinkedList<>();
        agents.add(getConfig("es.bsc.compss.agent.rest.RESTAgent", args[0]));

        for (AgentInterfaceConfig agent : agents) {
            try {
                startInterface(agent);
            } catch (Exception e) {
                ErrorManager.warn("Could not start Agent", e);
            }
        }
        if (INTERFACES.isEmpty()) {
            ErrorManager.fatal("Could not start any interface");
        }
    }
}
