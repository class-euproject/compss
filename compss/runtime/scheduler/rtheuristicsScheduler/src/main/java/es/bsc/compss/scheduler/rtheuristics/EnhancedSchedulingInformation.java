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
package es.bsc.compss.scheduler.rtheuristics;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class EnhancedSchedulingInformation extends SchedulingInformation {

    private boolean scheduled = false;

    // Allocatable actions that the action depends on due to resource availability
    private final List<AllocatableAction> resourcePredecessors;

    // Allocatable actions depending on the allocatable action due to resource availability
    private List<AllocatableAction> resourceSuccessors;

    private boolean hasOpAction;

    // indicates number of dependencies to opAction
    private int countOpAction = 0;


    /**
     * Creates a new EnhancedSchedulingInformation instance.
     * 
     * @param enforcedTargetResource Enforced target resource.
     */
    public <T extends WorkerResourceDescription> EnhancedSchedulingInformation(
        ResourceScheduler<T> enforcedTargetResource) {
        super(enforcedTargetResource);
        resourcePredecessors = new LinkedList<>();
        resourceSuccessors = new LinkedList<>();
        hasOpAction = false;
    }

    public void addPredecessor(AllocatableAction predecessor) {
        resourcePredecessors.add(predecessor);
    }

    public boolean hasOpActionPred() {
        return hasOpAction;
    }

    public void setHasOpAction(boolean value) {
        hasOpAction = value;
    }

    public boolean hasPredecessors() {
        return !resourcePredecessors.isEmpty();
    }

    public boolean hasSuccessors() {
        return !resourceSuccessors.isEmpty();
    }

    public List<AllocatableAction> getPredecessors() {
        return resourcePredecessors;
    }

    /**
     * Removes the predecessor of the given action.
     * 
     * @param predecessor Predecessor to remove.
     */
    public void removePredecessor(AllocatableAction predecessor) {
        Iterator<AllocatableAction> it = resourcePredecessors.iterator();
        AllocatableAction g = null;
        while (it.hasNext()) {
            g = it.next();
            if (g == predecessor) {
                it.remove();
            }
        }
    }

    /**
     * Clears all its predecessors.
     */
    public void clearPredecessors() {
        resourcePredecessors.clear();
    }

    public void addSuccessor(AllocatableAction successor) {
        resourceSuccessors.add(successor);
    }

    public List<AllocatableAction> getSuccessors() {
        return resourceSuccessors;
    }

    /**
     * Removes the successor of the given action.
     * 
     * @param successor Successor to remove.
     */
    public void removeSuccessor(AllocatableAction successor) {
        Iterator<AllocatableAction> it = resourceSuccessors.iterator();
        AllocatableAction g = null;
        while (it.hasNext()) {
            g = it.next();
            if (g == successor) {
                it.remove();
            }
        }
    }

    /**
     * Removes all successors.
     */
    public void clearSuccessors() {
        resourceSuccessors.clear();
    }

    /**
     * Replaces the current list of successors.
     * 
     * @param newSuccessors New list of successors.
     * @return Old successors.
     */
    public List<AllocatableAction> replaceSuccessors(List<AllocatableAction> newSuccessors) {
        List<AllocatableAction> oldSuccessors = resourceSuccessors;
        resourceSuccessors = newSuccessors;
        return oldSuccessors;
    }

    @Override
    public final boolean isExecutable() {
        return !hasPredecessors(); // if action does not have predecessors it can be executed; otherwise blocked
    }

    /**
     * Marks as scheduled.
     */
    public void scheduled() {
        scheduled = true;
    }

    /**
     * Unschedules the EnhancedSI.
     */
    public void unscheduled() {
        scheduled = false;
        resourcePredecessors.clear();
        resourceSuccessors.clear();
    }

    /**
     * Returns whether the EnhancedSI has been scheduled or not.
     * 
     * @return {@literal true} if the EnhancedSI has been scheduled, {@literal false} otherwise.
     */
    public boolean isScheduled() {
        return scheduled;
    }

    /**
     * Returns the number of dependencies the current action has with the opAction.
     * 
     * @return the countOpAction
     */
    public int getCountOpAction() {
        return countOpAction;
    }

    /**
     * Increments by one the number of dependencies the current action has with the opAction.
     */
    public void incCountOpAction() {
        this.countOpAction++;
    }

    /**
     * Decrements by one the number of dependencies the current action has with the opAction.
     */
    public void decCountOpAction() {
        this.countOpAction--;
    }

    @Override
    /**
     * Formats the object to string.
     * 
     * @return The string formed by the object attributes.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder("\tschedPredecessors: ");
        for (AllocatableAction g : getPredecessors()) {
            sb.append(" ").append(g);
        }
        sb.append("\n");
        sb.append("\t").append("schedSuccessors: ");
        for (AllocatableAction aa : getSuccessors()) {
            sb.append(" ").append(aa);
        }
        sb.append("\n");
        return sb.toString();
    }

}
