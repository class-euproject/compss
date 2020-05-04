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
package es.bsc.compss.util;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.scheduler.types.WorkloadState;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;


public class CustomResourceOptimizer extends ResourceOptimizer {

    // Loggers
    protected static final Logger RESOURCES_LOGGER = LogManager.getLogger(Loggers.RESOURCES);
    protected static final Logger RUNTIME_LOGGER = LogManager.getLogger(Loggers.RM_COMP);
    protected static final boolean DEBUG = RUNTIME_LOGGER.isDebugEnabled();

    // WARNING MESSAGES
    private static final String WARN_NO_RESOURCE_MATCHES = "WARN: No resource matches the constraints";
    private static final String WARN_NO_COMPATIBLE_TYPE = "WARN: Cannot find any compatible instanceType";
    private static final String WARN_NO_COMPATIBLE_IMAGE = "WARN: Cannot find any compatible Image";
    private static final String WARN_NO_VALID_INSTANCE = "WARN: Cannot find a containing/contained instanceType";
    private static final String WARN_NO_MORE_INSTANCES = "WARN: Cloud Provider cannot host more instances";
    private static final String WARN_NO_POSIBLE_INCREASE = "WARN: Cloud Provider cannot increase resources";
    private static final String WARN_EXCEPTION_TURN_ON = "WARN: Connector exception on turn on resource";

    // Sleep times
    private static final int SLEEP_TIME = 2_000;
    // This counts the number of retries when detecting a situation where all the tasks are blocked, and no resources
    // are being created/can be created. (if you don't handle this situation, the runtime gets blocked)
    // To don't consider the first execution we initialize the value to -1
    private int everythingBlockedRetryCount = -1;
    private long lastPotentialBlockedCheck = System.currentTimeMillis();
    private Map<CloudInstanceTypeDescription, CloudTypeProfile> defaultProfiles;


    /**
     * Creates a new ResourceOptimizer instance.
     *
     * @param ts Associated Task Scheduler.
     */
    public CustomResourceOptimizer(TaskScheduler ts) {
        super(ts);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************** INITIAL RESOURCES CREATIONS ******************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * Triggers the creation of the initial VMs to accomplish the expected minimum VMs and ensure that there are workers
     * to run every type of task.
     */
    @Override
    protected void initialCreations() {
        int alreadyCreated = addBasicNodes();
        // Distributes the rest of the VM
        addExtraNodes(alreadyCreated);
    }

    /**
     * Asks for the VM needed for the runtime to be able to execute all method cores. First it groups the constraints of
     * all the methods per Architecture and tries to merge included resource descriptions in order to reduce the amount
     * of required VMs. It also tries to join the unassigned architecture methods with the closer constraints of a
     * defined one. After that it distributes the Initial VM Count among the architectures taking into account the
     * number of methods that can be run in each architecture. If the amount of different constraints is higher than the
     * Initial VM count it applies an aggressive merge method to each architecture in order to fulfill the initial
     * Constraint. It creates a single VM for each final method constraint. Although these aggressive merges, the amount
     * of different constraints can be higher than the initial VM Count constraint. In this case, it violates the
     * initial VM constraint and asks for more resources.
     *
     * @return the amount of requested VM
     */
    public static int addBasicNodes() {
        int coreCount = CoreManager.getCoreCount();
        List<ConstraintsCore>[] unfulfilledConstraints = getUnfulfilledConstraints();
        int unfulfilledConstraintsCores = 0;
        for (int coreId = 0; coreId < coreCount; coreId++) {
            if (unfulfilledConstraints[coreId].size() > 0) {
                unfulfilledConstraintsCores += unfulfilledConstraints[coreId].size();
                break;
            }
        }
        if (unfulfilledConstraintsCores == 0) {
            return 0;
        }

        /*
         * constraintsPerArquitecture has loaded all constraint for each task. architectures has a list of all the
         * architecture names.
         *
         * e.g. architectures constraintsPerArquitecture Intel = |MR1|--|MR2| AMD = |MR3| [unassigned] = |MR4|--|MR5|
         */
        Map<String, List<ConstraintsCore>> arch2Constraints = classifyArchitectures(unfulfilledConstraints);

        /*
         * Tries to reduce the number of machines per architecture by entering constraints in another core's constraints
         */
        reduceArchitecturesConstraints(arch2Constraints);

        /*
         * Checks if there are enough Vm for a Unassigned Arquitecture If not it set each unassigned task into the
         * architecture with the most similar task e.g. constraintsPerArquitecture Intel --> |MR1|--|MR2|--|MR5| AMD -->
         * |MR3|--|MR4|
         */
        reassignUnassignedConstraints(arch2Constraints);

        /*
         * Tries to reduce the number of machines per architecture by entering constraints in another core's constraints
         */
        reduceArchitecturesConstraints(arch2Constraints);

        int createdCount = 0;
        for (int coreId = 0; coreId < coreCount; coreId++) {
            while (!unfulfilledConstraints[coreId].isEmpty()) {
                ConstraintsCore cc = unfulfilledConstraints[coreId].remove(0);
                cc.confirmed();
                ResourceCreationRequest rcr = askForResources(cc.desc);
                if (rcr != null) {
                    rcr.print(RESOURCES_LOGGER, DEBUG);
                    createdCount += rcr.getRequested().getReplicas();
                }
            }
        }

        if (DEBUG) {
            RESOURCES_LOGGER
                .debug("DEBUG_MSG = [\n" + "\tIn order to be able to execute all cores, Resource Manager has asked for "
                    + createdCount + " Cloud resources\n" + "]");
        }
        return createdCount;
    }

    // Removes from the list all the Constraints fulfilled by existing resources
    @SuppressWarnings("unchecked")
    private static List<ConstraintsCore>[] getUnfulfilledConstraints() {
        int coreCount = CoreManager.getCoreCount();
        List<ConstraintsCore>[] unfulfilledConstraints = new LinkedList[coreCount];
        int[] maxSimTasks = ResourceManager.getTotalSlots();
        for (CoreElement ce : CoreManager.getAllCores()) {
            int coreId = ce.getCoreId();
            unfulfilledConstraints[coreId] = new LinkedList<>();
            if (maxSimTasks[coreId] == 0) {
                List<Implementation> impls = ce.getImplementations();
                for (Implementation impl : impls) {
                    if (impl.getTaskType() == TaskType.METHOD) {
                        MethodResourceDescription requirements = (MethodResourceDescription) impl.getRequirements();
                        CloudMethodResourceDescription cd = new CloudMethodResourceDescription(requirements);
                        ConstraintsCore cc = new ConstraintsCore(cd, coreId, unfulfilledConstraints[coreId]);
                        unfulfilledConstraints[coreId].add(cc);
                    }
                }
            }
        }
        return unfulfilledConstraints;
    }

    /**
     * Classifies the constraints depending on their architecture and leaves it on coreResourceList.
     *
     * @param constraints Constraints to classify.
     * @return List with all the architectures' names.
     */
    private static Map<String, List<ConstraintsCore>> classifyArchitectures(List<ConstraintsCore>[] constraints) {
        Map<String, List<ConstraintsCore>> archs = new HashMap<>();

        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            if (constraints[coreId] != null) {
                for (ConstraintsCore cc : constraints[coreId]) {
                    List<String> runnableArchitectures = cc.desc.getArchitectures();
                    for (String arch : runnableArchitectures) {
                        // Insert element into HashMap
                        List<ConstraintsCore> archConstr = archs.get(arch);
                        if (archConstr == null) {
                            archConstr = new LinkedList<>();
                            archs.put(arch, archConstr);
                        }
                        archConstr.add(cc);
                    }
                }
            }
        }
        return archs;
    }

    private static void reduceArchitecturesConstraints(Map<String, List<ConstraintsCore>> arch2Ctrs) {
        for (List<ConstraintsCore> arch : arch2Ctrs.values()) {
            ConstraintsCore[] ctrs = new ConstraintsCore[arch.size()];
            int i = 0;
            for (ConstraintsCore cc : arch) {
                ctrs[i++] = cc;
            }

            Integer[] mergedTo = new Integer[arch.size()];
            for (i = 0; i < ctrs.length; i++) {
                if (mergedTo[i] != null) {
                    continue;
                }
                String os = ctrs[i].desc.getOperatingSystemType();
                for (int j = i + 1; j < ctrs.length; j++) {
                    if (mergedTo[j] != null) {
                        continue;
                    }
                    if (os.compareTo(ctrs[j].desc.getOperatingSystemType()) == 0
                        || os.compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0 || ctrs[j].desc
                            .getOperatingSystemType().compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0) {
                        mergedTo[j] = i;
                        ctrs[i].join(ctrs[j]);
                        arch.remove(ctrs[j]);
                    }
                }
            }
        }

    }

    private static void reassignUnassignedConstraints(Map<String, List<ConstraintsCore>> arch2Ctrs) {
        /*
         * ATTENTION: Since this method is only evaluated from constraints, there is only 1 PROCESSOR and 1 ARCHITECTURE
         */
        List<ConstraintsCore> unassignedList = arch2Ctrs.get(CloudMethodResourceDescription.UNASSIGNED_STR);
        if (unassignedList == null) {
            return;
        }
        if (arch2Ctrs.size() == 1) {
            return;
        }
        if (arch2Ctrs.size() == 2) {
            for (Entry<String, List<ConstraintsCore>> ctrs : arch2Ctrs.entrySet()) {
                if (ctrs.getKey().compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0) {
                    continue;
                } else {
                    ctrs.getValue().addAll(unassignedList);
                    return;
                }
            }
        }

        List<ConstraintsCore> assignedList = new LinkedList<>();
        for (Entry<String, List<ConstraintsCore>> ctrs : arch2Ctrs.entrySet()) {
            if (ctrs.getKey().compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0) {
                continue;
            } else {
                assignedList.addAll(ctrs.getValue());

            }
        }

        while (!unassignedList.isEmpty()) {
            ConstraintsCore unassigned = unassignedList.remove(0);
            CloudMethodResourceDescription candidate = unassigned.desc;
            String bestArch = CloudMethodResourceDescription.UNASSIGNED_STR;
            Float bestDifference = Float.MAX_VALUE;
            for (ConstraintsCore assigned : assignedList) {
                CloudMethodResourceDescription option = assigned.desc;
                float difference = candidate.difference(option);
                if (bestDifference < 0) {
                    if (difference < 0) {
                        if (difference > bestDifference) {
                            List<String> availArchs = option.getArchitectures();
                            if (availArchs != null && !availArchs.isEmpty()) {
                                bestArch = availArchs.get(0);
                            }
                            bestDifference = difference;
                        }
                    }
                } else if (difference < bestDifference) {
                    List<String> availArchs = option.getArchitectures();
                    if (availArchs != null && !availArchs.isEmpty()) {
                        bestArch = availArchs.get(0);
                    }
                    bestDifference = difference;
                }
            }

            // Add
            List<Processor> procs = unassigned.desc.getProcessors();
            if (procs == null) {
                procs = new LinkedList<>();
            }

            if (!procs.isEmpty()) {
                procs.get(0).setArchitecture(bestArch);
            } else {
                Processor p = new Processor();
                p.setArchitecture(bestArch);
                procs.add(p);
            }
            arch2Ctrs.get(bestArch).add(unassigned);
        }
    }

    /**
     * Asks for the rest of VM that user wants to start with. After executing the addBasicNodes, it might happen that
     * the number of initial VMs constrained by the user is still not been fulfilled. The addBasicNodes creates up to as
     * much VMs as different methods. If the initial VM Count is higher than this number of methods then there will be
     * still some VM requests missing. The addExtraNodes creates this difference of VMs. First it tries to merge the
     * method constraints that are included into another methods. And performs a less aggressive and more equal
     * distribution.
     *
     * @param alreadyCreated number of already requested VMs.
     * @return the number of extra VMs created to fulfill the Initial VM Count constraint.
     */
    public static int addExtraNodes(int alreadyCreated) {
        // int initialVMsCount = ResourceManager.getInitialCloudVMs();
        int initialVMsCount = 0;
        int vmCount = initialVMsCount - alreadyCreated;
        if (vmCount <= 0) {
            return 0;
        }

        if (DEBUG) {
            RESOURCES_LOGGER.debug("DEBUG_MSG = [\n" + "\tALREADY_CREATED_INSTANCES = " + alreadyCreated + "\n"
                + "\tMAXIMUM_NEW_PETITIONS = " + vmCount + "\n" + "]");
        }

        /*
         * Tries to reduce the number of machines by entering methodConstraints in another method's machine
         */
        /*
         * LinkedList<CloudWorkerDescription> requirements = new LinkedList<CloudWorkerDescription>(); for (int coreId =
         * 0; coreId < CoreManager.coreCount; coreId++) { Implementation impl =
         * CoreManager.getCoreImplementations(coreId)[0]; if (impl.getType() == Type.METHOD) { WorkerDescription wd =
         * (WorkerDescription) impl.getRequirements(); requirements.add(new CloudWorkerDescription(wd)); } } if
         * (requirements.size() == 0) { return 0; } requirements = reduceConstraints(requirements);
         *
         * int numTasks = requirements.size(); int[] vmCountPerContraint = new int[numTasks]; int[]
         * coreCountPerConstraint = new int[numTasks];
         *
         * for (int index = 0; index < numTasks; index++) { vmCountPerContraint[index] = 1;
         * coreCountPerConstraint[index] = requirements.get(index).getSlots(); }
         *
         * for (int i = 0; i < vmCount; i++) { float millor = 0.0f; int opcio = 0; for (int j = 0; j <
         * requirements.size(); j++) { if (millor < ((float) coreCountPerConstraint[j] / (float)
         * vmCountPerContraint[j])) { opcio = j; millor = ((float) coreCountPerConstraint[j] / (float)
         * vmCountPerContraint[j]); } } ResourceCreationRequest rcr =
         * CloudManager.askForResources(requirements.get(opcio), false);
         *
         * LOGGER.info( "CREATION_ORDER = [\n" + "\tTYPE = " + rcr.getRequested().getType() + "\n" + "\tPROVIDER = " +
         * rcr.getProvider() + "\n" + "\tREASON = Fulfill the initial Cloud instances constraint\n" + "]"); if (DEBUG) {
         * StringBuilder sb = new StringBuilder("EXPECTED_INSTANCE_SIM_TASKS = ["); int[][] simultaneousImpls =
         * rcr.requestedSimultaneousTaskCount(); for (int core = 0; core < simultaneousImpls.length; ++core) { int
         * simultaneousTasks = 0; for (int j = 0; j < simultaneousImpls[core].length; ++j) { if (simultaneousTasks <
         * simultaneousImpls[core][j]) { simultaneousTasks = simultaneousImpls[core][j]; } }
         * sb.append("\t").append("CORE = [").append("\n"); sb.append("\t").append
         * ("\t").append("COREID = ").append(core).append("\n"); sb.append("\t").
         * append("\t").append("SIM_TASKS = ").append(simultaneousTasks ).append("\n");
         * sb.append("\t").append("]").append("\n"); sb.append(", ").append(simultaneousTasks); } sb.append("]");
         * logger.debug(sb.toString()); }
         *
         * vmCountPerContraint[opcio]++; }
         */
        return vmCount;
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************** DYNAMIC RESOURCES CREATIONS ******************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    protected void applyPolicies(WorkloadState workload) {
        int currentCloudVMCount = ResourceManager.getCurrentVMCount();
        Integer initialVRs = ResourceManager.getInitialVRs();
        Integer maximumVRs = ResourceManager.getMaximumVRs();

        long creationTime;
        try {
            creationTime = ResourceManager.getNextCreationTime();
        } catch (Exception ex) {
            creationTime = 120_000L;
        }

        if (creationTime < 30_000) {
            creationTime = 30_000L;
        }

        int coreCount = workload.getCoreCount();
        int noResourceCount = workload.getNoResourceCount();
        int[] noResourceCounts = workload.getNoResourceCounts();

        long[] minCoreTime = new long[coreCount];
        long[] meanCoreTime = new long[coreCount];
        long[] maxCoreTime = new long[coreCount];
        long[] minRemainingCoreTime = new long[coreCount];
        long[] meanRemainingCoreTime = new long[coreCount];
        long[] maxRemainingCoreTime = new long[coreCount];

        for (int coreId = 0; coreId < coreCount; coreId++) {
            minCoreTime[coreId] = Math.min(workload.getCoreMinTime(coreId), creationTime);
            meanCoreTime[coreId] = Math.min(workload.getCoreMeanTime(coreId), creationTime);
            maxCoreTime[coreId] = Math.min(workload.getCoreMaxTime(coreId), creationTime);
            long meanRunningCoreTime = workload.getRunningCoreMeanTime(coreId);
            if (minCoreTime[coreId] - meanRunningCoreTime < 0) {
                minRemainingCoreTime[coreId] = 0;
            } else {
                minRemainingCoreTime[coreId] = Math.min(minCoreTime[coreId] - meanRunningCoreTime, creationTime);
            }
            if (meanCoreTime[coreId] - meanRunningCoreTime < 0) {
                meanRemainingCoreTime[coreId] = 0;
            } else {
                meanRemainingCoreTime[coreId] = Math.min(meanCoreTime[coreId] - meanRunningCoreTime, creationTime);
            }
            if (maxCoreTime[coreId] - meanRunningCoreTime < 0) {
                maxRemainingCoreTime[coreId] = 0;
            } else {
                maxRemainingCoreTime[coreId] = Math.min(maxCoreTime[coreId] - meanRunningCoreTime, creationTime);
            }

        }
        long[] runningMinCoreTime = new long[coreCount];
        long[] runningMeanCoreTime = new long[coreCount];
        long[] runningMaxCoreTime = new long[coreCount];
        long[] readyMinCoreTime = new long[coreCount];
        long[] readyMeanCoreTime = new long[coreCount];
        long[] readyMaxCoreTime = new long[coreCount];
        long[] pendingMinCoreTime = new long[coreCount];
        long[] pendingMeanCoreTime = new long[coreCount];
        long[] pendingMaxCoreTime = new long[coreCount];

        int[] realSlots = ResourceManager.getAvailableSlots();
        int[] totalSlots = ResourceManager.getTotalSlots();

        int[] runningCounts = workload.getRunningTaskCounts();
        int[] readyCounts = workload.getReadyCounts();
        // int[] pendingCounts = workload.getWaitingTaskCounts();
        int[] pendingCounts = new int[coreCount];

        long totalPendingTasks = 0;
        int maxConcurrentTasks = 0;
        for (int i = 0; i < coreCount; i++) {
            runningMinCoreTime[i] = minRemainingCoreTime[i] * runningCounts[i];
            readyMinCoreTime[i] = runningMinCoreTime[i] + (minCoreTime[i] * readyCounts[i]);
            pendingMinCoreTime[i] = readyMinCoreTime[i] + (minCoreTime[i] * pendingCounts[i]);
            runningMeanCoreTime[i] = meanRemainingCoreTime[i] * runningCounts[i];
            readyMeanCoreTime[i] = runningMeanCoreTime[i] + (meanCoreTime[i] * readyCounts[i]);
            pendingMeanCoreTime[i] = readyMeanCoreTime[i] + (meanCoreTime[i] * pendingCounts[i]);
            runningMaxCoreTime[i] = maxRemainingCoreTime[i] * runningCounts[i];
            readyMaxCoreTime[i] = runningMaxCoreTime[i] + (maxCoreTime[i] * readyCounts[i]);
            pendingMaxCoreTime[i] = readyMaxCoreTime[i] + (maxCoreTime[i] * pendingCounts[i]);
            totalPendingTasks = totalPendingTasks + pendingCounts[i];
            if (runningCounts[i] > 0 || readyCounts[i] > 0) {
                if (realSlots[i] > maxConcurrentTasks) {
                    maxConcurrentTasks = realSlots[i];
                }
            }

        }
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Applying VM optimization policies (currentVMs: "
                + currentCloudVMCount + " maxVMs: " + maximumVRs + " minVMs: " + initialVRs + ")");
        }

        // TODO: This should be done on a CloudProvider basis now

        // Check if there is some mandatory creation/destruction
        if (!cleanUp) {
            // For CE without resources where to run
            List<Integer> requiredVMs = checkNeededMachines(noResourceCount, noResourceCounts, totalSlots);

            if (!requiredVMs.isEmpty()) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Required VMs. Mandatory Increase");
                float[] creationRecommendations = recommendCreations(coreCount, creationTime, readyMinCoreTime,
                    readyMeanCoreTime, readyMaxCoreTime, totalSlots, realSlots);
                for (Integer coreId : requiredVMs) {
                    // Ensure that we ask one slot for it
                    creationRecommendations[coreId] = Math.max(creationRecommendations[coreId], 1);
                }
                mandatoryIncrease(creationRecommendations, requiredVMs);
                return;
            }

            // For accomplishing the minimum amount of vms
            if (initialVRs != null && initialVRs > currentCloudVMCount) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount
                    + ") count smaller than minimum VMs (" + initialVRs + "). Mandatory Increase");
                float[] creationRecommendations = orderCreations(coreCount, creationTime, readyMinCoreTime,
                    readyMeanCoreTime, readyMaxCoreTime, totalSlots, realSlots);
                mandatoryIncrease(creationRecommendations, new LinkedList<>());
                return;
            }
            // For not exceeding the VM top limit
            if (maximumVRs != null && maximumVRs < currentCloudVMCount) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount
                    + ") count bigger than maximum VMs (" + maximumVRs + "). Mandatory reduction");
                float[] destroyRecommendations = deleteRecommendations(coreCount, SLEEP_TIME, pendingMinCoreTime,
                    pendingMeanCoreTime, pendingMaxCoreTime, totalSlots, realSlots);
                mandatoryReduction(destroyRecommendations);
                return;
            }
        }

        // Check Recommended creations
        if ((maximumVRs == null || maximumVRs > currentCloudVMCount) && workload.getReadyCount() > 1) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount
                + ") count smaller than maximum VMs (" + maximumVRs + ")");
            float[] creationRecommendations = recommendCreations(coreCount, creationTime, readyMinCoreTime,
                readyMeanCoreTime, readyMaxCoreTime, totalSlots, realSlots);
            if (optionalIncrease(creationRecommendations)) {
                return;
            }
        }
        // Check Recommended destructions
        if ((initialVRs == null || initialVRs < currentCloudVMCount)
            && totalPendingTasks <= workload.getReadyCount() + workload.getRunningTaskCount()
            && workload.getReadyCount() + workload.getRunningTaskCount() <= maxConcurrentTasks) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount
                + ") count bigger than minimum VMs (" + initialVRs + ")");
            float[] destroyRecommendations = deleteRecommendations(coreCount, SLEEP_TIME, pendingMinCoreTime,
                pendingMeanCoreTime, pendingMaxCoreTime, totalSlots, realSlots);
            if (optionalReduction(destroyRecommendations)) {
                return;
            }
        }
    }

    private void mandatoryIncrease(float[] creationRecommendations, List<Integer> requiredVMs) {
        PriorityQueue<ValueResourceDescription> pq = new PriorityQueue<>();

        boolean[] required = new boolean[creationRecommendations.length];
        for (int coreId : requiredVMs) {
            required[coreId] = true;
        }

        for (int coreId = 0; coreId < creationRecommendations.length; coreId++) {
            CoreElement ce = CoreManager.getCore(coreId);
            for (Implementation impl : ce.getImplementations()) {
                if (impl.getTaskType() == TaskType.SERVICE) {
                    continue;
                }

                MethodResourceDescription constraints = ((MethodImplementation) impl).getRequirements();
                ValueResourceDescription v =
                    new ValueResourceDescription(constraints, creationRecommendations[coreId], false);
                pq.add(v);
            }
        }
        requestOneCreation(pq, true);
    }

    private boolean optionalIncrease(float[] creationRecommendations) {
        PriorityQueue<ValueResourceDescription> pq = new PriorityQueue<>();

        for (int coreId = 0; coreId < creationRecommendations.length; coreId++) {
            if (creationRecommendations[coreId] > 1) {
                CoreElement ce = CoreManager.getCore(coreId);
                for (Implementation impl : ce.getImplementations()) {
                    if (impl.getTaskType() == TaskType.SERVICE) {
                        continue;
                    }
                    MethodResourceDescription constraints = ((MethodImplementation) impl).getRequirements();
                    ValueResourceDescription v =
                        new ValueResourceDescription(constraints, creationRecommendations[coreId], false);
                    pq.add(v);
                }
            }
        }

        ResourceCreationRequest rcr = requestOneCreation(pq, false);
        return (rcr != null);
    }

    private static ResourceCreationRequest requestOneCreation(PriorityQueue<ValueResourceDescription> pq,
        boolean include) {
        ValueResourceDescription v;
        while ((v = pq.poll()) != null) {
            ResourceCreationRequest rcr = askForResources(v.constraints, include);
            if (rcr != null) {
                rcr.print(RESOURCES_LOGGER, DEBUG);
                return rcr;
            }
        }

        return null;
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ******************************* DYNAMIC RESOURCES RECOMMENDATIONS ***************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    private List<Integer> checkNeededMachines(int noResourceCount, int[] noResourceCountPerCore,
        int[] slotCountPerCore) {
        List<Integer> needed = new LinkedList<>();
        if (noResourceCount == 0) {
            return needed;
        }
        for (int i = 0; i < CoreManager.getCoreCount(); i++) {
            if (noResourceCountPerCore[i] > 0 && slotCountPerCore[i] == 0) {
                needed.add(i);
            }
        }
        return needed;
    }

    private float[] recommendCreations(int coreCount, long creationTime, long[] aggregatedMinCoreTime,
        long[] aggregatedMeanCoreTime, long[] aggregatedMaxCoreTime, int[] totalSlots, int[] realSlots) {

        float[] creations = new float[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            // long realTime = realSlots[coreId] * creationTime;
            long totalTime = totalSlots[coreId] * creationTime;
            // OLD version: did a mean of total (include pending creations) and
            // real
            // long embraceableLoad = (realTime + totalTime) / 2;
            long embraceableLoad = totalTime;
            long remainingLoad = aggregatedMeanCoreTime[coreId] - embraceableLoad;
            if (DEBUG) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] * Calculating increase recomendations");
                RUNTIME_LOGGER.debug("\tRemaining load = " + aggregatedMeanCoreTime[coreId] + "-( " + totalSlots[coreId]
                    + " * " + creationTime + " ) = " + remainingLoad);
            }
            if (remainingLoad > 0) {
                creations[coreId] = (int) (remainingLoad / creationTime);
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("\tRecomended slots = " + creations[coreId] + " ( " + remainingLoad + " / "
                        + creationTime + " )");
                }
            } else {
                creations[coreId] = 0;
            }
        }
        return creations;
    }

    private float[] orderCreations(int coreCount, long creationTime, long[] aggregatedMinCoreTime,
        long[] aggregatedMeanCoreTime, long[] aggregatedMaxCoreTime, int[] totalSlots, int[] realSlots) {

        float[] creations = new float[coreCount];
        int maxI = 0;
        float maxRatio = 0;
        for (int i = 0; i < CoreManager.getCoreCount(); i++) {
            if (aggregatedMeanCoreTime[i] > 0 && totalSlots[i] > 0) {
                float ratio = aggregatedMeanCoreTime[i] / totalSlots[i];
                if (ratio > maxRatio) {
                    maxI = i;
                    maxRatio = ratio;
                }
            }
        }
        creations[maxI] = 1;
        return creations;
    }

    private float[] deleteRecommendations(int coreCount, long limitTime, long[] aggregatedMinCoreTime,
        long[] aggregatedMeanCoreTime, long[] aggregatedMaxCoreTime, int[] totalSlots, int[] realSlots) {

        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] * Delete Recomendations calculations:\n\tcoreCount: " + coreCount
                + " limitTime: " + limitTime);
        }
        float[] destructions = new float[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {

            long embraceableLoad = limitTime * (realSlots[coreId]);

            if (embraceableLoad == 0L) {
                destructions[coreId] = 0;
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("\tembraceableLoad [ " + coreId + "] = " + limitTime + " * "
                        + realSlots[coreId] + " = " + embraceableLoad);
                    RUNTIME_LOGGER.debug("\tdestructions [ " + coreId + "] = 0");
                }
            } else if (aggregatedMinCoreTime[coreId] > 0) {
                double unusedTime = (((double) embraceableLoad) / (double) 2) - (double) aggregatedMeanCoreTime[coreId];
                destructions[coreId] = (float) (unusedTime / (double) limitTime);
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("\tembraceableLoad [ " + coreId + "] = " + limitTime + " * "
                        + realSlots[coreId] + " = " + embraceableLoad);
                    RUNTIME_LOGGER.debug("\tunused [ " + coreId + "] =  ( " + embraceableLoad + " / 2) - "
                        + aggregatedMeanCoreTime[coreId] + " = " + unusedTime);
                    RUNTIME_LOGGER.debug("\tdestructions [ " + coreId + "] = " + destructions[coreId]);
                }
            } else {
                destructions[coreId] = embraceableLoad / limitTime;
            }

        }
        return destructions;
    }

    /**
     * Asks for resources.
     *
     * @param cp Cloud provider.
     * @param instanceName Instance name to ask.
     * @param imageName Image name to ask.
     * @return ResourceCreationRequest to ask for more resources.
     */
    public ResourceCreationRequest askForResources(CloudProvider cp, String instanceName, String imageName) {
        CloudMethodResourceDescription constraints = cp.getResourceDescription(instanceName, imageName);
        if (constraints == null) {
            RUNTIME_LOGGER.warn(WARN_EXCEPTION_TURN_ON);
            return null;
        }

        return cp.requestResourceCreation(constraints);
    }

    /**
     * The CloudManager ask for resources that can execute certain amount of cores at the same time. It checks the best
     * resource that each provider can offer to execute that amount of cores and picks one of them. It constructs a
     * resourceRequest describing the resource and which cores can be executed on it. This ResourceRequest will be used
     * to ask for that resource creation to the Cloud Provider and returned if the application is accepted.
     *
     * @param requirements Features of the resource.
     * @return A ResourceCreationRequest.
     */
    public static ResourceCreationRequest askForResources(MethodResourceDescription requirements) {
        // Search best resource
        CloudProvider bestProvider = null;
        CloudMethodResourceDescription bestConstraints = null;
        Float bestValue = Float.MAX_VALUE;
        for (CloudProvider cp : ResourceManager.getAvailableCloudProviders()) {
            CloudMethodResourceDescription rc = getBestIncreaseOnProvider(cp, requirements);
            if (rc != null && rc.getValue() < bestValue) {
                bestProvider = cp;
                bestConstraints = rc;
                bestValue = rc.getValue();
            } else if (rc != null && bestConstraints == null) {
                bestProvider = cp;
                bestConstraints = rc;
                bestValue = rc.getValue();
            } else {
                RUNTIME_LOGGER.warn(WARN_NO_POSIBLE_INCREASE + " (" + cp.getName() + ")");
            }
        }
        if (bestConstraints == null) {
            RUNTIME_LOGGER.warn(WARN_NO_RESOURCE_MATCHES);
            return null;
        }

        return bestProvider.requestResourceCreation(bestConstraints);
    }

    /**
     * Return the best increase on the CloudProvider to fit the requirements.
     *
     * @param cp Cloud Provider.
     * @param requirements Requirements to fit.
     * @return A MethodResourceDescription describing the best increase to fit the requirements.
     */
    public static CloudMethodResourceDescription getBestIncreaseOnProvider(CloudProvider cp,
        MethodResourceDescription requirements) {
        int amount = cp.getInitialVRs();
        if (cp.getCurrentVMCount() + amount > cp.getMaximumVRs()) {
            amount = cp.getMaximumVRs() - cp.getCurrentVMCount();
        }

        RUNTIME_LOGGER.debug("[Resource Optimizer] Getting best increase in provider " + cp.getName());
        // Check Cloud capabilities
        if (!cp.canHostMoreInstances()) {
            RUNTIME_LOGGER.warn(WARN_NO_MORE_INSTANCES);
            return null;
        }

        // Select all the compatible types
        List<CloudInstanceTypeDescription> instances = cp.getCompatibleTypes(requirements);
        RUNTIME_LOGGER.debug("[Resource Optimizer] There are " + instances.size() + " instances compatible.");
        if (instances.isEmpty()) {
            RUNTIME_LOGGER.warn(WARN_NO_COMPATIBLE_TYPE);
            return null;
        }

        CloudMethodResourceDescription result = null;
        CloudInstanceTypeDescription type = selectContainingInstance(instances, requirements, amount);

        // Pick an image to be loaded in the Type (or return null)
        if (type != null) {
            // Select all the compatible images
            List<CloudImageDescription> images = cp.getCompatibleImages(requirements);
            RUNTIME_LOGGER.debug("[Resource Optimizer] There are " + images.size() + " images compatible.");
            if (!images.isEmpty()) {
                CloudImageDescription image = images.get(0);
                result = new CloudMethodResourceDescription(type, image, amount);
                result.setValue(cp.getInstanceCostPerHour(result));
            } else {
                RUNTIME_LOGGER.warn(WARN_NO_COMPATIBLE_IMAGE);
            }
        } else {
            RUNTIME_LOGGER.warn(WARN_NO_VALID_INSTANCE);
        }

        return result;
    }

    protected static CloudInstanceTypeDescription selectContainingInstance(List<CloudInstanceTypeDescription> instances,
        MethodResourceDescription constraints, int amount) {
        return instances.get(0);
    }


    private class ReductionOption {

        private final Resource res;

        private float undesiredCEsAffected = 0;// values[0]
        private float undesiredSlotsAffected = 0; // values[1]
        private float desiredSlotsAffected = 0; // values[2]


        public ReductionOption(Resource res) {
            this.res = res;
        }

        public void undesiredCEAffected() {
            undesiredCEsAffected++;
        }

        public void undesiredSlotsAffected(float slots) {
            undesiredSlotsAffected += slots;
        }

        public void desiredSlotsAffected(float slots) {
            desiredSlotsAffected += slots;
        }

        public float getUndesiredCEsAffected() {
            return undesiredCEsAffected;
        }

        public float getUndesiredSlotsAffected() {
            return undesiredSlotsAffected;
        }

        public float getDesiredSlotsAffected() {
            return desiredSlotsAffected;
        }

        public String getName() {
            return res.getName();
        }

        public void apply() {
            ResourceManager.requestWholeWorkerReduction((DynamicMethodWorker) res);
        }

        public Resource getResource() {
            return res;
        }

    }

    private class CloudReductionOption extends ReductionOption {

        private final CloudInstanceTypeDescription type;


        public CloudReductionOption(CloudInstanceTypeDescription type, Resource res) {
            super(res);
            this.type = type;
        }

        @Override
        public String getName() {
            return type.getName() + "@" + super.getName();
        }

        @Override
        public void apply() {
            CloudMethodWorker res = ((CloudMethodWorker) getResource());
            CloudMethodResourceDescription cmrd = (CloudMethodResourceDescription) res.getDescription();
            CloudImageDescription cid = cmrd.getImage();

            CloudMethodResourceDescription finalDescription = new CloudMethodResourceDescription(type, cid);
            finalDescription.setName(res.getName());
            ResourceManager.requestWorkerReduction(res, finalDescription);
        }
    }

    // Private Classes
    protected class CloudTypeProfile {

        private Profile[][] implProfiles;


        public CloudTypeProfile(JSONObject typeJSON, JSONObject implsJSON) {
            this.implProfiles = loadProfiles(typeJSON, implsJSON);
        }

        public Profile getImplProfiles(int coreId, int implId) {
            return this.implProfiles[coreId][implId];
        }

        /**
         * Prepares the default profiles for each implementation cores.
         *
         * @param resMap default profile values for the resource.
         * @param implMap default profile values for the implementation.
         * @return default profile structure.
         */
        private final Profile[][] loadProfiles(JSONObject resMap, JSONObject implMap) {
            Profile[][] profiles;
            int coreCount = CoreManager.getCoreCount();
            profiles = new Profile[coreCount][];
            for (CoreElement ce : CoreManager.getAllCores()) {
                int coreId = ce.getCoreId();
                List<Implementation> impls = ce.getImplementations();
                int implCount = impls.size();
                profiles[coreId] = new Profile[implCount];
                for (Implementation impl : impls) {
                    String signature = impl.getSignature();
                    JSONObject jsonImpl = null;
                    if (resMap != null) {
                        try {
                            jsonImpl = resMap.getJSONObject(signature);
                            profiles[coreId][impl.getImplementationId()] =
                                generateProfileForImplementation(impl, jsonImpl);
                        } catch (JSONException je) {
                            // Do nothing
                        }
                    }
                    if (profiles[coreId][impl.getImplementationId()] == null) {
                        if (implMap != null) {
                            try {
                                jsonImpl = implMap.getJSONObject(signature);
                            } catch (JSONException je) {
                                // Do nothing
                            }
                        }
                        profiles[coreId][impl.getImplementationId()] = generateProfileForImplementation(impl, jsonImpl);
                        profiles[coreId][impl.getImplementationId()].clearExecutionCount();
                    }
                }
            }
            return profiles;
        }

        /**
         * Generates a Profile for an action.
         *
         * @param impl Action implementation.
         * @param jsonImpl JSON Implementation definition.
         * @return A profile object for an action.
         */
        protected Profile generateProfileForImplementation(Implementation impl, JSONObject jsonImpl) {
            return new Profile(jsonImpl);
        }

    }

    private static class ConstraintsCore {

        private CloudMethodResourceDescription desc;
        private List<ConstraintsCore>[] cores;


        @SuppressWarnings("unchecked")
        public ConstraintsCore(CloudMethodResourceDescription desc, int core, List<ConstraintsCore> coreList) {
            this.desc = desc;
            this.cores = new LinkedList[CoreManager.getCoreCount()];
            this.cores[core] = coreList;
        }

        public void join(ConstraintsCore c2) {
            desc.increase(c2.desc);
            c2.desc = desc;
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                if (cores[coreId] != null) {
                    if (c2.cores[coreId] != null) {
                        // Remove one instance of the list to avoid replication
                        cores[coreId].remove(c2);
                    } else {
                        c2.cores[coreId] = cores[coreId];
                    }
                } else {
                    cores[coreId] = c2.cores[coreId];
                }
            }
        }

        public void confirmed() {
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                if (cores[coreId] != null) {
                    cores[coreId].clear();
                }
            }
        }

        @Override
        public String toString() {
            List<Integer> cores = new LinkedList<>();
            for (int i = 0; i < CoreManager.getCoreCount(); i++) {
                if (this.cores[i] != null) {
                    cores.add(i);
                }
            }
            return desc.toString() + " meets constraints for cores " + cores;
        }
    }

    private static class ValueResourceDescription implements Comparable<ValueResourceDescription> {

        private final MethodResourceDescription constraints;
        private final float value;
        private final boolean prioritary;


        public ValueResourceDescription(MethodResourceDescription constraints, float value, boolean prioritary) {
            this.constraints = constraints;
            this.value = value;
            this.prioritary = prioritary;
        }

        @Override
        public int compareTo(ValueResourceDescription o) {
            if (this.prioritary && !o.prioritary) {
                return 1;
            }

            if (!this.prioritary && o.prioritary) {
                return -1;
            }

            float dif = value - o.value;
            if (dif > 0) {
                return 1;
            } else if (dif < 0) {
                return -1;
            } else {
                return 0;
            }
        }

        @Override
        public String toString() {
            return value + (prioritary ? "!" : "") + constraints;
        }
    }

}
