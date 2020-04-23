/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.paper;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.heuristics.LNSNL;
import es.bsc.compss.scheduler.heuristics.Result;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.allocatableactions.ExecutionAction;
import es.bsc.compss.types.allocatableactions.TransferValueAction;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode.
 */
public class PaperScheduler extends TaskScheduler {

    // map that relates each task with a predefined resource, according to the output of the Paper
    private LinkedHashMap<Long, String> taskResource;

    // map that contains, for each resource, the order of the task execution given by the ILP
    private LinkedHashMap<String, ArrayList<AllocatableAction>> orderInWorkers;

    // map where key is name of the resource and value is the OpAction
    private LinkedHashMap<String, OptimizationAction> opActions;

    // iters: array of ints indicating position of ExecutionAction in the ArrayList inside map variable
    // ex: ExecutionAction 8 access iters[8] which will contain the actual position inside the
    // map.get("resourceId").get())
    private int[] iters;

    private int numTasks;

    private LNSNL lnsnl;

    private List<AllocatableAction> unassignedActions;

    private List<AbstractMap.SimpleEntry<String, String>> cloudWorkers;

    private boolean noWorkers;


    /**
     * Constructs a new Paper Scheduler instance.
     */
    public PaperScheduler() {
        super();
        PaperConfiguration.load();
        // readInputFile();
        this.unassignedActions = new ArrayList<>();
        this.orderInWorkers = new LinkedHashMap<>();
        this.cloudWorkers = new ArrayList<>();
        noWorkers = true;
    }

    private void readInputFile() {
        taskResource = new LinkedHashMap<>();
        this.numTasks = 0;
        try (FileReader fr = new FileReader(new File(PaperConfiguration.INPUT_FILE));
            BufferedReader br = new BufferedReader(fr)) {
            String lines;
            String resourceId;
            long taskId;
            while (!(lines = br.readLine()).trim().split("\\s+")[0].equals("Resource")) {
                String[] st = lines.trim().split("\\s+");
                taskId = Long.parseLong(st[0]);
                resourceId = st[1];
                taskResource.put(taskId, resourceId); // taskId is unique
                this.numTasks++;
            }
            iters = new int[taskResource.size()];
            orderInWorkers = new LinkedHashMap<>();
            opActions = new LinkedHashMap<>();
            String resource = lines.trim().split("\\s+")[1];
            while ((lines = br.readLine()) != null) {
                if (lines.trim().split("\\s+")[0].equals("Resource")) {
                    resource = lines.trim().split("\\s+")[1];
                    continue;
                }
                taskId = Long.parseLong(lines);
                if (orderInWorkers.containsKey(resource)) {
                    orderInWorkers.get(resource).add(null);
                } else {
                    ArrayList<AllocatableAction> orderedTasks = new ArrayList<>();
                    orderedTasks.add(null);
                    orderInWorkers.put(resource, orderedTasks);
                    OptimizationAction opAction = new OptimizationAction();
                    opActions.put(resource, opAction);
                }
                iters[(int) taskId - 1] = orderInWorkers.get(resource).size() - 1; // ExecAction order in the array
                // by iters array
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public String getResourceForTask(long taskId) {
        return taskResource.get(taskId);
    }

    private void printMap() {
        for (Map.Entry<Long, String> pair : taskResource.entrySet()) {
            LOGGER.debug("[PaperScheduler] Reading content from file " + pair.getKey() + " " + pair.getValue());
        }

        for (int i = 0; i < iters.length; i++) {
            LOGGER.debug("[PaperScheduler] Order of task " + (i + 1) + " in resource " + iters[i]);
        }
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public <T extends WorkerResourceDescription> PaperResourceScheduler<T> generateSchedulerForResource(Worker<T> w,
        JSONObject resJSON, JSONObject implJSON) {
        LOGGER.debug("[PaperScheduler] Generate scheduler for resource " + w.getName());
        noWorkers = false;
        if (w instanceof CloudMethodWorker) {
            LOGGER.debug("[PaperScheduler] CLOUD METHOD WORKER " + w.getName());
            cloudWorkers
                .add(new AbstractMap.SimpleEntry<>(w.getName(), ((CloudMethodWorker) w).getProvider().getName()));
        }
        return new PaperResourceScheduler<>(w, resJSON, implJSON, this);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        LOGGER.debug("[PaperScheduler] Generate Action Score for " + action);
        return new Score(action.getPriority(), 0, 0, 0, 0);
    }

    @Override
    public <T extends WorkerResourceDescription> EnhancedSchedulingInformation
        generateSchedulingInformation(ResourceScheduler<T> enforcedTargetResource) {
        return new EnhancedSchedulingInformation(enforcedTargetResource);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    private void initializeHeuristics() {
        List<String> workingWorkerList =
            this.workers.values().stream().map(ResourceScheduler::getName).collect(Collectors.toList());
        if (workingWorkerList.isEmpty()) { // no workers still available to run
            noWorkers = true;
        } else if (lnsnl == null) {
            lnsnl = new LNSNL(workingWorkerList);
            this.numTasks = lnsnl.getNumTasks();
            for (AbstractMap.SimpleEntry<String, String> worker : cloudWorkers) {
                lnsnl.addResourceCloud(worker.getKey(), worker.getValue());
            }
            Result res = lnsnl.schedule();
            updateInternalStructures(res);
        } else { // lnsnl already set, remove previous tasks
            for (String name : orderInWorkers.keySet()) {
                orderInWorkers.get(name).replaceAll(e -> null);
            }
        }
        // TODO: check if correct for considering new workers deployed after first worker present and refactor

        /*
         * if (!noWorkers && lnsnl != null) { System.out.println("HAHAAHAHAHAHAH I ENTER HEREE"); for
         * (AbstractMap.SimpleEntry<String, String> worker : cloudWorkers) { lnsnl.addResourceCloud(worker.getKey(),
         * worker.getValue()); } Result res = lnsnl.schedule(); updateInternalStructures(res); }
         */
    }

    private void updateInternalStructures(Result res) {
        taskResource = res.getTaskResource();
        LinkedHashMap<String, ArrayList<Integer>> mapRes = res.getOrderOfTasks();
        this.opActions = new LinkedHashMap<>();
        for (String name : mapRes.keySet()) {
            ArrayList<Integer> aux = mapRes.get(name);
            if (aux != null) {
                ArrayList<AllocatableAction> value = new ArrayList<>(Collections.nCopies(aux.size(), null));
                orderInWorkers.put(name, value);
            }
            OptimizationAction opAction = new OptimizationAction();
            this.opActions.put(name, opAction);
        }
        iters = res.getIters();
    }

    @Override
    protected void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {
        LOGGER.debug("[PaperScheduler] Scheduling action " + action);
        if (!noWorkers) {
            int id = ((ExecutionAction) action).getTask().getId();
            if (id == 1 || (numTasks != 0 && (((id - 1) % numTasks) + 1) == 1)) {
                initializeHeuristics();
            }
            id = ((id - 1) % numTasks) + 1;
            String name = getResourceForTask(id);
            addDependenciesToAction(action, name, id);
            advanceTransfers(action);
            try {
                action.schedule(workers.get(ResourceManager.getWorker(name)), actionScore);
            } catch (UnassignedActionException uae) {
                LOGGER.warn("[PaperScheduler] Action " + action + " is unassigned");
                this.unassignedActions.add(action);
            }
        } else {
            this.unassignedActions.add(action);
        }
    }

    // updates the internal maps for the TaskScheduler in order to add both data and resource dependencies
    // to the action being scheduled
    private void addDependenciesToAction(AllocatableAction action, String name, int id) {
        int pos = iters[id - 1];
        LOGGER.debug("[PaperScheduler] Position for task " + id + " in ordered list: " + pos);
        fillDependencies(action, name, pos);
        orderInWorkers.get(name).set(pos, action); // add the action received in order to be able to
        // access it when its successors arrive

        ArrayList<AllocatableAction> predecessors = orderInWorkers.get(name);
        while (pos > 0) {
            AllocatableAction pred = predecessors.get(pos - 1);
            if (pred != null) {
                Task predTask = ((ExecutionAction) pred).getTask();
                // add pred/succ resource dependency
                LOGGER.debug("[PaperScheduler] Task predecessor for task " + id + " is " + predTask.getId());

                if (predTask.getStatus() == TaskState.FINISHED) {
                    // if a previous action has finished, it means that all the others will have finished as well
                    break;
                }

                LOGGER.debug("[PaperScheduler] Adding dependencies for task " + id + " with task " + predTask.getId());
                ((EnhancedSchedulingInformation) action.getSchedulingInfo()).addPredecessor(pred);
                ((EnhancedSchedulingInformation) pred.getSchedulingInfo()).addSuccessor(action);
            } else {
                // if pred null but not the first position, meaning that previous task not yet received
                LOGGER.debug("[PaperScheduler] Task " + id
                    + " not the first one or predecessor not yet created. Adding fake dependencies");
                OptimizationAction opAction = opActions.get(name);
                ((EnhancedSchedulingInformation) opAction.getSchedulingInfo()).addSuccessor(action);
                ((EnhancedSchedulingInformation) action.getSchedulingInfo()).addPredecessor(opAction);
                ((EnhancedSchedulingInformation) action.getSchedulingInfo()).setHasOpAction(true);
                ((EnhancedSchedulingInformation) action.getSchedulingInfo()).incCountOpAction();
            }
            pos--;
        }

    }

    private void removeSuccessors(ArrayList<AllocatableAction> succToRemove,
        EnhancedSchedulingInformation opActionDSI) {
        for (AllocatableAction succ : succToRemove) {
            opActionDSI.removeSuccessor(succ);
        }
    }

    private boolean visitedSuccessor(ArrayList<AllocatableAction> visited, AllocatableAction action) {
        for (AllocatableAction act : visited) {
            if (((ExecutionAction) action).getTask().getId() == ((ExecutionAction) act).getTask().getId()) {
                return true;
            }
        }
        return false;
    }

    // tries to remove dependencies from the opAction and add that successor to the received action
    private void fillDependencies(AllocatableAction action, String name, int posAction) {
        OptimizationAction optAction = opActions.get(name);
        if (optAction != null) {
            EnhancedSchedulingInformation opActionDSI = ((EnhancedSchedulingInformation) optAction.getSchedulingInfo());

            if (opActionDSI.hasSuccessors()) {
                // check if the successor should be added any dependency to the actual task received -> action
                ArrayList<AllocatableAction> succToRemove = new ArrayList<>();
                ArrayList<AllocatableAction> visited = new ArrayList<>();
                for (AllocatableAction succ : opActionDSI.getSuccessors()) {
                    if (visitedSuccessor(visited, succ)) {
                        continue;
                    }
                    visited.add(succ);

                    int idSucc = ((((ExecutionAction) succ).getTask().getId() - 1) % numTasks) + 1;
                    int posSucc = iters[idSucc - 1];
                    if (posSucc >= posAction) {
                        EnhancedSchedulingInformation actionDSI =
                            ((EnhancedSchedulingInformation) action.getSchedulingInfo());
                        EnhancedSchedulingInformation succDSI =
                            ((EnhancedSchedulingInformation) succ.getSchedulingInfo());
                        actionDSI.addSuccessor(succ);
                        succDSI.addPredecessor(action);

                        // if success on first step, delete dependency with the optimization action
                        succDSI.decCountOpAction();
                        succDSI.removePredecessor(optAction);
                        if (succDSI.getCountOpAction() == 0) {
                            succToRemove.add(succ);
                            succDSI.setHasOpAction(false);
                        }
                    }
                }
                removeSuccessors(succToRemove, opActionDSI);
            }
        }
    }

    // create TransferValueActions for each parameter needed
    private void advanceTransfers(AllocatableAction action) {
        int id = ((((ExecutionAction) action).getTask().getId() - 1) % numTasks) + 1;
        if (action.hasDataPredecessors()) {
            TaskDescription tD = ((ExecutionAction) action).getTask().getTaskDescription();
            LOGGER.debug("[PaperScheduler] Task description for actual task " + id + " is " + tD);
            for (Parameter p : tD.getParameters()) {
                if (p.getDirection() != Direction.OUT) {
                    for (AllocatableAction predecessor : action.getDataPredecessors()) {
                        TaskDescription taskDescription =
                            ((ExecutionAction) predecessor).getTask().getTaskDescription();

                        List<Parameter> params = taskDescription.getParameters();

                        for (Parameter param : params) {
                            if (param.getDirection() != Direction.IN) {
                                DependencyParameter dp = (DependencyParameter) param;
                                if (p instanceof DependencyParameter
                                    && ((DependencyParameter) p).getOriginalName().equals(dp.getOriginalName())) {
                                    EnhancedSchedulingInformation schedInfo = new EnhancedSchedulingInformation(null);
                                    ActionOrchestrator orchestrator = predecessor.getActionOrchestrator();
                                    dp = (DependencyParameter) p;
                                    String name = getResourceForTask(id);
                                    TransferValueAction transfer = new TransferValueAction(schedInfo, orchestrator, dp,
                                        workers.get(ResourceManager.getWorker(name)));

                                    if ((((ExecutionAction) predecessor).getTask().getStatus() != TaskState.FINISHED)) {
                                        transfer.addDataPredecessor(predecessor);
                                    }
                                    Score transferScore = generateActionScore(transfer);
                                    try {
                                        transfer.schedule(transferScore);
                                    } catch (BlockedActionException bae) {
                                        ErrorManager.warn("[PaperScheduler] Blocked scheduling for transfer action "
                                            + predecessor + " " + dp);
                                    } catch (UnassignedActionException ise) {
                                        ErrorManager.warn("[PaperScheduler] Unassigned scheduling for transfer action "
                                            + predecessor + " " + dp);
                                    }
                                    try {
                                        transfer.tryToLaunch();
                                    } catch (InvalidSchedulingException ise) {
                                        ErrorManager.warn("[PaperScheduler] Invalid scheduling for transfer action "
                                            + predecessor + " " + dp);
                                    }
                                    break; // for a specific parameter only one predecessor,
                                    // so once it has been found stop
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public int getTaskPosition(long taskId) {
        return iters[(int) taskId - 1];
    }

    public int getNumTasks() {
        return numTasks;
    }

    /**
     * Returns the task Id of the new task to be executed.
     *
     * @param resourceName Name of the resource.
     * @param taskId Id of the task that has finished its execution.
     */
    public AllocatableAction getNextTaskForResource(String resourceName, long taskId) {
        ArrayList<AllocatableAction> orderedTasks = orderInWorkers.get(resourceName);
        int pos = iters[(int) taskId - 1] + 1;
        AllocatableAction nextTask;
        if (pos == orderedTasks.size()) { // last task to be executed on that resource
            nextTask = null;
        } else { // there are still tasks left to be executed on the resource
            nextTask = orderedTasks.get(pos);
        }
        return nextTask;
    }

    @Override
    public <T extends WorkerResourceDescription> void handleDependencyFreeActions(
        List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
        List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {

        Set<AllocatableAction> freeTasks = new HashSet<>();
        freeTasks.addAll(resourceFreeActions);
        freeTasks.addAll(dataFreeActions);
        for (AllocatableAction freeAction : freeTasks) {
            EnhancedSchedulingInformation freeDSI = (EnhancedSchedulingInformation) freeAction.getSchedulingInfo();

            if (!freeDSI.hasPredecessors()) {
                // LOGGER.warn("Action " + freeAction + " try to launch");
                tryToLaunch(freeAction);
            }
        }

        Iterator<AllocatableAction> unassignedIterator = this.unassignedActions.iterator();
        synchronized (this) {
            while (unassignedIterator.hasNext()) {
                AllocatableAction freeAction = unassignedIterator.next();
                unassignedIterator.remove();
                try {
                    scheduleAction(freeAction, generateActionScore(freeAction));
                    tryToLaunch(freeAction);
                } catch (BlockedActionException bae) {
                    LOGGER.warn("[PrometheusScheduler] Action " + freeAction + " blocked");
                }
            }
        }
    }

}
