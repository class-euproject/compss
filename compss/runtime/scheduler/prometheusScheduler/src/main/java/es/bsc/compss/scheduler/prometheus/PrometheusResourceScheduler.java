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
package es.bsc.compss.scheduler.prometheus;

import es.bsc.compss.NIOProfile;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.ActionNotFoundException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.DeadlineMonitor;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.allocatableactions.ExecutionAction;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import org.json.JSONObject;


public class PrometheusResourceScheduler<T extends WorkerResourceDescription> extends ResourceScheduler<T> {

    private PrometheusScheduler scheduler;


    /**
     * New resource scheduler instance.
     *
     * @param w Worker.
     * @param resJSON Resource JSON file.
     * @param implJSON Implementation JSON file.
     */
    public PrometheusResourceScheduler(Worker<T> w, JSONObject resJSON, JSONObject implJSON,
        PrometheusScheduler sched) {
        super(w, resJSON, implJSON);
        this.scheduler = sched;
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */

    @Override
    public PriorityQueue<AllocatableAction> getBlockedActions() {
        PriorityQueue<AllocatableAction> blockedActions =
            new PriorityQueue<>(20, (a1, a2) -> generateBlockedScore(a1).compareTo(generateBlockedScore(a2)));

        blockedActions.addAll(super.getBlockedActions());
        return blockedActions;
    }

    @Override
    public Score generateBlockedScore(AllocatableAction action) {
        LOGGER.debug("[PrometheusResourceScheduler] Generate blocked score for action " + action);

        long taskId = ((((ExecutionAction) action).getTask().getId() - 1) % this.scheduler.getNumTasks()) + 1;
        if (!((EnhancedSchedulingInformation) action.getSchedulingInfo()).hasPredecessors()) {
            return new Score(-this.scheduler.getTaskPosition(taskId), 0, 0, 0, 0);
        } else {
            return new Score(this.scheduler.getTaskPosition(taskId), 0, 0, 0, 0);
        }
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        LOGGER.debug("[PrometheusResourceScheduler] Generate resource score for action " + action + " in resource "
            + this.getName());

        long actionPriority = actionScore.getPriority();
        long resourceScore;
        long taskId = ((((ExecutionAction) action).getTask().getId() - 1) % this.scheduler.getNumTasks()) + 1;
        if (Objects.equals(this.getName(), this.scheduler.getResourceForTask(taskId))) {
            resourceScore = 100;
        } else {
            resourceScore = Long.MIN_VALUE;
        }
        long waitingScore = 0;
        long implementationScore = 0;
        long actionGroupPriority = 0;

        return new Score(actionPriority, actionGroupPriority, resourceScore, waitingScore, implementationScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl,
        Score resourceScore) {

        LOGGER.debug("[PrometheusResourceScheduler] Generate implementation score for action " + action);
        if (resourceScore == null || resourceScore.getResourceScore() == Long.MIN_VALUE) {
            return null;
        }
        long actionPriority = resourceScore.getPriority();
        long resourcePriority;
        long taskId = ((((ExecutionAction) action).getTask().getId() - 1) % this.scheduler.getNumTasks()) + 1;
        if (Objects.equals(this.getName(), this.scheduler.getResourceForTask(taskId))) {
            resourcePriority = 100;
        } else {
            return null;
        }
        long waitingScore = 0;
        long implScore = 0;
        long actionGroupPriority = 0;
        return new Score(actionPriority, actionGroupPriority, resourcePriority, waitingScore, implScore);
    }

    @Override
    public List<AllocatableAction> unscheduleAction(AllocatableAction action) throws ActionNotFoundException {

        super.unscheduleAction(action);
        LOGGER.debug("[PrometheusResourceScheduler] Unschedule action " + action + " on resource " + getName());

        setZeroTime(action);
        checkDeadlines(action);

        EnhancedSchedulingInformation actionDSI = (EnhancedSchedulingInformation) action.getSchedulingInfo();
        for (AllocatableAction pred : actionDSI.getPredecessors()) {
            if (pred != null) {
                EnhancedSchedulingInformation predDSI = (EnhancedSchedulingInformation) pred.getSchedulingInfo();
                predDSI.removeSuccessor(action);
            }
        }
        actionDSI.clearPredecessors();

        LinkedList<AllocatableAction> resourceFree = new LinkedList<>();
        for (AllocatableAction successor : actionDSI.getSuccessors()) {
            EnhancedSchedulingInformation succDSI = (EnhancedSchedulingInformation) successor.getSchedulingInfo();
            succDSI.removePredecessor(action);
            List<AllocatableAction> predecessors = succDSI.getPredecessors();
            long taskId = ((((ExecutionAction) successor).getTask().getId() - 1) % this.scheduler.getNumTasks()) + 1;
            if (!succDSI.hasPredecessors()) { // if no resource predecessors, add to be ready to execute
                if (resourceFree.isEmpty()) {
                    resourceFree = new LinkedList<>();
                }
                resourceFree.add(successor);
                LOGGER.debug("[PrometheusResourceScheduler] Next action to execute is " + taskId);
            } else if (succDSI.hasOpActionPred() && predecessors.size() == 1 && resourceFree.isEmpty()) {
                // if its only predecessor is the opAction, next one to execute
                Iterator it = predecessors.iterator();
                EnhancedSchedulingInformation opActionDSI =
                    (EnhancedSchedulingInformation) ((AllocatableAction) it.next()).getSchedulingInfo();
                opActionDSI.removeSuccessor(successor);
                succDSI.clearPredecessors();
                succDSI.setHasOpAction(false);
                resourceFree.add(successor);
                LOGGER.debug(
                    "[PrometheusResourceScheduler] Next action to execute is " + taskId + " on resource " + getName());
            }
        }
        actionDSI.clearSuccessors();
        return resourceFree;
    }

    private void setZeroTime(AllocatableAction action) {
        if (action instanceof ExecutionAction) {
            int numTasks = this.scheduler.getNumTasks();
            int id = ((((ExecutionAction) action).getTask().getId() - 1) % numTasks) + 1;
            boolean secondRound = this.scheduler.isSecondRound();
            if (secondRound) {
                TaskMonitor monitor = ((ExecutionAction) action).getTask().getTaskMonitor();
                if (monitor instanceof DeadlineMonitor) {
                    // long zeroTime = ((DeadlineMonitor) monitor).getProfile().getStartTime();
                    long zeroTime = ((DeadlineMonitor) monitor).getProfile().getStartTimeMaster();
                    LOGGER.debug("[PrometheusResourceScheduler] Zero time is " + zeroTime);
                    this.scheduler.setZeroTime(zeroTime);
                    // System.out.println("ZERO TIME IS " + zeroTime);
                    // TODO: zero time is the first task of the workflow that reaches NIOAdaptor
                    // -> (((taskId - 1) % numTasks) + 1) == 1
                }
            }
        }
    }

    private void checkDeadlines(AllocatableAction action) {
        String ansiReset = "\u001B[0m";
        String ansiRed = "\u001B[31m";
        String ansiGreen = "\u001B[32m";

        if (action instanceof ExecutionAction) {
            Task task = ((ExecutionAction) action).getTask();
            long taskId = ((task.getId() - 1) % this.scheduler.getNumTasks()) + 1;

            TaskMonitor monitor = task.getTaskMonitor();
            if (monitor instanceof DeadlineMonitor) {
                NIOProfile p = ((DeadlineMonitor) monitor).getProfile();
                // long endTime = p.getEndTime();
                long endTime = p.getEndTimeMaster();
                // long startTime = p.getStartTime();
                long startTime = p.getStartTimeMaster();

                LOGGER.debug("[PrometheusResourceScheduler] Action " + taskId + " started at " + startTime
                    + ", ended at " + endTime + " and lasted " + (endTime - startTime));

                long zeroTime = this.scheduler.getZeroTime();
                if (zeroTime != -1) { // if zero time not set, do not check the deadlines (skipping first iter)
                    float inTime = this.scheduler.getEndTime(taskId);
                    if (endTime - zeroTime > inTime) {
                        System.out.println(ansiRed + "WARNING: Deadline has been missed for task " + taskId
                            + " on resource " + getName() + " (End time: " + (endTime - zeroTime)
                            + "; Expected end time: " + inTime + ")" + ansiReset);

                        new Thread(() -> {
                            this.scheduler.incrementTaskCounter();
                        }).start();
                    } else {
                        System.out.println(ansiGreen + "Deadline has been successful for task " + taskId
                            + " on resource " + getName() + " (End time: " + (endTime - zeroTime)
                            + "; Expected end time: " + inTime + ")" + ansiReset);
                    }
                }
            }
        }
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "PrometheusResourceScheduler@" + getName();
    }

}
