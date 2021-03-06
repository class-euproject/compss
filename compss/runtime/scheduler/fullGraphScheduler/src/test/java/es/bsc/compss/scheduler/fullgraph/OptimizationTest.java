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
package es.bsc.compss.scheduler.fullgraph;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.fullgraph.FullGraphResourceScheduler;
import es.bsc.compss.scheduler.fullgraph.FullGraphScheduler;
import es.bsc.compss.scheduler.fullgraph.FullGraphSchedulingInformation;
import es.bsc.compss.scheduler.fullgraph.ScheduleOptimizer;
import es.bsc.compss.scheduler.fullgraph.utils.Verifiers;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.OptimizationWorker;
import es.bsc.compss.scheduler.types.PriorityActionSet;
import es.bsc.compss.scheduler.types.fake.FakeActionOrchestrator;
import es.bsc.compss.scheduler.types.fake.FakeAllocatableAction;
import es.bsc.compss.scheduler.types.fake.FakeImplDefinition;
import es.bsc.compss.scheduler.types.fake.FakeImplementation;
import es.bsc.compss.scheduler.types.fake.FakeProfile;
import es.bsc.compss.scheduler.types.fake.FakeResourceDescription;
import es.bsc.compss.scheduler.types.fake.FakeResourceScheduler;
import es.bsc.compss.scheduler.types.fake.FakeWorker;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ResourceManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import org.junit.BeforeClass;
import org.junit.Test;


public class OptimizationTest {

    private static FullGraphScheduler ds;
    private static FakeActionOrchestrator fao;
    private static FullGraphResourceScheduler<FakeResourceDescription> drs1;
    private static FullGraphResourceScheduler<FakeResourceDescription> drs2;


    /**
     * Tests the optimization phase.
     */
    public OptimizationTest() {
        ds = new FullGraphScheduler();
        fao = new FakeActionOrchestrator(ds);
        ds.setOrchestrator(fao);
    }

    /**
     * To setup the class.
     */
    @BeforeClass
    public static void setUpClass() {
        ResourceManager.clear(null);

        CoreManager.clear();

        CoreElementDefinition ced;
        FakeImplDefinition fid;

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature00");
        fid = new FakeImplDefinition("fakeSignature00", new FakeResourceDescription(2));
        ced.addImplementation(fid);
        CoreElement ce0 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl00 = ce0.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature10");
        fid = new FakeImplDefinition("fakeSignature10", new FakeResourceDescription(3));
        ced.addImplementation(fid);
        CoreElement ce1 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl10 = ce1.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature20");
        fid = new FakeImplDefinition("fakeSignature20", new FakeResourceDescription(1));
        ced.addImplementation(fid);
        CoreElement ce2 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl20 = ce2.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature30");
        fid = new FakeImplDefinition("fakeSignature30", new FakeResourceDescription(4));
        ced.addImplementation(fid);
        CoreElement ce3 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl30 = ce3.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature40");
        fid = new FakeImplDefinition("fakeSignature40", new FakeResourceDescription(2));
        ced.addImplementation(fid);
        CoreElement ce4 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl40 = ce4.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature50");
        fid = new FakeImplDefinition("fakeSignature50", new FakeResourceDescription(1));
        ced.addImplementation(fid);
        CoreElement ce5 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl50 = ce5.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature60");
        fid = new FakeImplDefinition("fakeSignature60", new FakeResourceDescription(3));
        ced.addImplementation(fid);
        CoreElement ce6 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl60 = ce6.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature70");
        CoreManager.registerNewCoreElement(ced);

        int maxSlots = 4;
        FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
        FakeWorker fw = new FakeWorker("worker1", frd, maxSlots);
        drs1 = new FullGraphResourceScheduler<>(fw, null, null, fao);

        FakeResourceDescription frd2 = new FakeResourceDescription(maxSlots);
        FakeWorker fw2 = new FakeWorker("worker2", frd2, maxSlots);
        drs2 = new FullGraphResourceScheduler<>(fw2, null, null, fao);

        drs1.profiledExecution(impl00, new FakeProfile(50));
        drs1.profiledExecution(impl10, new FakeProfile(50));
        drs1.profiledExecution(impl20, new FakeProfile(30));
        drs1.profiledExecution(impl30, new FakeProfile(50));
        drs1.profiledExecution(impl40, new FakeProfile(20));
        drs1.profiledExecution(impl50, new FakeProfile(10));
        drs1.profiledExecution(impl60, new FakeProfile(30));

        drs2.profiledExecution(impl00, new FakeProfile(50));
        drs2.profiledExecution(impl10, new FakeProfile(50));
        drs2.profiledExecution(impl20, new FakeProfile(30));
        // Faster than drs
        drs2.profiledExecution(impl30, new FakeProfile(30));
        drs2.profiledExecution(impl40, new FakeProfile(15));
        drs2.profiledExecution(impl50, new FakeProfile(10));
        drs2.profiledExecution(impl60, new FakeProfile(15));
    }

    // @Test
    /**
     * Tests the donors and receivers.
     */
    @SuppressWarnings("unchecked")
    public void testDonorsAndReceivers() {
        ScheduleOptimizer so = new ScheduleOptimizer(ds);

        long[] expectedEndTimes = new long[] { 35000, 20000, 15000, 50000, 40000, 1000 };
        OptimizationWorker<?>[] optimizedWorkers = new OptimizationWorker[expectedEndTimes.length];
        for (int idx = 0; idx < expectedEndTimes.length; idx++) {
            int maxSlots = 1;
            FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
            FakeWorker fw = new FakeWorker("worker" + idx, frd, maxSlots);
            FakeResourceScheduler frs = new FakeResourceScheduler(fw, null, null, fao, expectedEndTimes[idx]);
            optimizedWorkers[idx] = new OptimizationWorker<>(frs);
        }

        LinkedList<OptimizationWorker<?>> receivers = new LinkedList<>();
        final LinkedList<OptimizationWorker<FakeResourceDescription>> receiversF = new LinkedList<>();
        OptimizationWorker<FakeResourceDescription> donor = (OptimizationWorker<FakeResourceDescription>) so
                .determineDonorAndReceivers(optimizedWorkers, receivers);

        LinkedList<OptimizationWorker<FakeResourceDescription>> donors = new LinkedList<>();
        donors.offer(donor);
        LinkedList<String> donorsNames = new LinkedList<>();
        donorsNames.add("worker3");

        LinkedList<String> receiversNames = new LinkedList<>();
        receiversNames.add("worker5");
        receiversNames.add("worker2");
        receiversNames.add("worker1");
        receiversNames.add("worker0");
        receiversNames.add("worker4");

        Verifiers.verifyWorkersPriority(donors, donorsNames);
        Verifiers.verifyWorkersPriority(receiversF, receiversNames);
    }

    // @Test
    /**
     * Tests the global optimization.
     */
    public void globalOptimization() {
        final ScheduleOptimizer so = new ScheduleOptimizer(ds);

        final long updateId = System.currentTimeMillis();

        Collection<ResourceScheduler<?>> workers = new ArrayList<>();
        drs1.clear();
        drs2.clear();
        workers.add(drs1);

        CoreElement ce4 = CoreManager.getCore(4);
        List<Implementation> ce4Impls = ce4.getImplementations();

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, ce4Impls);
        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        drs1.scheduleAction(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
            // Nothing to do
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, ce4Impls);
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
            // Nothing to do
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 1, ce4Impls);
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);

        CoreElement ce5 = CoreManager.getCore(5);
        List<Implementation> ce5Impls = ce5.getImplementations();

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, ce5Impls);
        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        drs1.scheduleAction(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 1, ce4Impls);
        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        drs1.scheduleAction(action5);

        CoreElement ce6 = CoreManager.getCore(6);
        List<Implementation> ce6Impls = ce6.getImplementations();

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 1, ce6Impls);
        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        drs1.scheduleAction(action6);

        workers.add(drs2);

        so.globalOptimization(updateId, workers);
    }

    /**
     * Tests the scan phase.
     */
    // @Test
    public void testScan() {
        CoreElement ce4 = CoreManager.getCore(4);
        List<Implementation> ce4Impls = ce4.getImplementations();

        FakeAllocatableAction external10 = new FakeAllocatableAction(fao, 13, 0, ce4Impls);
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).setExpectedEnd(10);
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).scheduled();
        external10.selectExecution(drs2, (FakeImplementation) external10.getImplementations()[0]);

        FakeAllocatableAction external20 = new FakeAllocatableAction(fao, 14, 0, ce4Impls);
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).setExpectedEnd(20);
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).scheduled();
        external20.selectExecution(drs2, (FakeImplementation) external20.getImplementations()[0]);

        FakeAllocatableAction external90 = new FakeAllocatableAction(fao, 15, 0, ce4Impls);
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).setExpectedEnd(90);
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).scheduled();
        external90.selectExecution(drs2, (FakeImplementation) external90.getImplementations()[0]);

        drs1.clear();
        drs2.clear();

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, ce4Impls);
        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        drs1.scheduleAction(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
            // Nothing to do
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, ce4Impls);
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
            // Nothing to do
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 1, ce4Impls);
        action3.addDataPredecessor(external90);
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);

        CoreElement ce5 = CoreManager.getCore(5);
        List<Implementation> ce5Impls = ce5.getImplementations();

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, ce5Impls);
        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        drs1.scheduleAction(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 1, ce4Impls);
        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        drs1.scheduleAction(action5);

        CoreElement ce6 = CoreManager.getCore(6);
        List<Implementation> ce6Impls = ce6.getImplementations();

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 1, ce6Impls);
        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        drs1.scheduleAction(action6);

        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, ce5Impls);
        action7.addDataPredecessor(external10);
        action7.selectExecution(drs1, (FakeImplementation) action7.getImplementations()[0]);
        drs1.scheduleAction(action7);

        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, ce5Impls);
        action8.addDataPredecessor(external20);
        action8.selectExecution(drs1, (FakeImplementation) action8.getImplementations()[0]);
        drs1.scheduleAction(action8);

        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, ce4Impls);
        action9.addDataPredecessor(external90);
        action9.selectExecution(drs1, (FakeImplementation) action9.getImplementations()[0]);
        drs1.scheduleAction(action9);

        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, ce4Impls);
        action10.addDataPredecessor(action5);
        action10.selectExecution(drs1, (FakeImplementation) action10.getImplementations()[0]);
        drs1.scheduleAction(action10);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, ce4Impls);
        action11.addDataPredecessor(action6);
        action11.selectExecution(drs1, (FakeImplementation) action11.getImplementations()[0]);
        drs1.scheduleAction(action11);

        FakeAllocatableAction action12 = new FakeAllocatableAction(fao, 12, 0, ce4Impls);
        action12.addDataPredecessor(action5);
        action12.addDataPredecessor(action6);
        action12.selectExecution(drs1, (FakeImplementation) action12.getImplementations()[0]);
        drs1.scheduleAction(action12);

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<>(1,
                FullGraphResourceScheduler.getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet selectableActions = new PriorityActionSet(FullGraphResourceScheduler.getScanComparator());

        drs1.scanActions(readyActions, selectableActions);

        HashMap<AllocatableAction, Long> expectedReady = new HashMap<>();
        expectedReady.put(action7, 10L);
        expectedReady.put(action8, 20L);
        expectedReady.put(action9, 90L);
        expectedReady.put(action3, 90L);
        Verifiers.verifyReadyActions(new PriorityQueue<>(readyActions), expectedReady);

        AllocatableAction[] expectedSelectable = new AllocatableAction[] { action5, action6, action4 };
        Verifiers.verifyPriorityActions(new PriorityActionSet(selectableActions), expectedSelectable);
    }

    /**
     * Prints the given action.
     * 
     * @param action Action to print.
     */
    public void printAction(AllocatableAction action) {
        System.out.println(action + " Core Element " + action.getCoreId() + " Implementation "
                + action.getAssignedImplementation().getImplementationId() + " (" + action.getAssignedImplementation()
                + ")");
        FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) action.getSchedulingInfo();
        System.out.println("\t Optimization:" + dsi.isOnOptimization());
        System.out.println("\t StartTime:" + dsi.getExpectedStart());
        System.out.println("\t EndTime:" + dsi.getExpectedEnd());
        System.out.println("\t Locks:" + dsi.getLockCount());
        System.out.println("\t Predecessors:" + dsi.getPredecessors());
        System.out.println("\t Successors:" + dsi.getSuccessors());
        System.out.println("\t Optimization Successors:" + dsi.getOptimizingSuccessors());

    }

    // @Test
    /**
     * Tests the pending actions.
     */
    public void testPendingActions() {
        final LinkedList<AllocatableAction> pendingActions = new LinkedList<>();

        CoreElement ce4 = CoreManager.getCore(4);
        List<Implementation> ce4Impls = ce4.getImplementations();

        FakeAllocatableAction external10 = new FakeAllocatableAction(fao, 13, 0, ce4Impls);
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).setExpectedEnd(10);
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).scheduled();
        external10.selectExecution(drs2, (FakeImplementation) external10.getImplementations()[0]);

        FakeAllocatableAction external20 = new FakeAllocatableAction(fao, 14, 0, ce4Impls);
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).setExpectedEnd(20);
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).scheduled();
        external20.selectExecution(drs2, (FakeImplementation) external20.getImplementations()[0]);

        FakeAllocatableAction external90 = new FakeAllocatableAction(fao, 15, 0, ce4Impls);
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).setExpectedEnd(90);
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).scheduled();
        external90.selectExecution(drs2, (FakeImplementation) external90.getImplementations()[0]);

        drs1.clear();
        drs2.clear();

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, ce4Impls);
        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        drs1.scheduleAction(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
            // Nothing to do
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, ce4Impls);
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
            // Nothing to do
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 1, ce4Impls);
        action3.addDataPredecessor(external90);
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);

        CoreElement ce5 = CoreManager.getCore(5);
        List<Implementation> ce5Impls = ce5.getImplementations();

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, ce5Impls);
        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        drs1.scheduleAction(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 1, ce4Impls);
        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        drs1.scheduleAction(action5);

        CoreElement ce6 = CoreManager.getCore(6);
        List<Implementation> ce6Impls = ce6.getImplementations();

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 1, ce6Impls);
        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        drs1.scheduleAction(action6);

        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, ce5Impls);
        action7.addDataPredecessor(external10);
        action7.selectExecution(drs1, (FakeImplementation) action7.getImplementations()[0]);
        pendingActions.add(action7);

        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, ce5Impls);
        action8.addDataPredecessor(external20);
        action8.selectExecution(drs1, (FakeImplementation) action8.getImplementations()[0]);
        pendingActions.add(action8);

        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, ce4Impls);
        action9.addDataPredecessor(external90);
        action9.selectExecution(drs1, (FakeImplementation) action9.getImplementations()[0]);
        pendingActions.add(action9);

        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, ce4Impls);
        action10.addDataPredecessor(action5);
        action10.selectExecution(drs1, (FakeImplementation) action10.getImplementations()[0]);
        pendingActions.add(action10);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, ce4Impls);
        action11.addDataPredecessor(action6);
        action11.selectExecution(drs1, (FakeImplementation) action11.getImplementations()[0]);
        pendingActions.add(action11);

        FakeAllocatableAction action12 = new FakeAllocatableAction(fao, 12, 0, ce4Impls);
        action12.addDataPredecessor(action5);
        action12.addDataPredecessor(action6);
        action12.selectExecution(drs1, (FakeImplementation) action12.getImplementations()[0]);
        pendingActions.add(action12);

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<>(1,
                FullGraphResourceScheduler.getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet selectableActions = new PriorityActionSet(FullGraphResourceScheduler.getScanComparator());

        drs1.scanActions(readyActions, selectableActions);
        drs1.classifyPendingSchedulings(pendingActions, readyActions, selectableActions, new LinkedList<>());

        HashMap<AllocatableAction, Long> expectedReady = new HashMap<>();
        expectedReady.put(action7, 10L);
        expectedReady.put(action8, 20L);
        expectedReady.put(action9, 90L);
        expectedReady.put(action3, 90L);
        Verifiers.verifyReadyActions(new PriorityQueue<>(readyActions), expectedReady);

        AllocatableAction[] expectedSelectable = new AllocatableAction[] { action5, action6, action4 };
        Verifiers.verifyPriorityActions(new PriorityActionSet(selectableActions), expectedSelectable);
    }

    @SuppressWarnings("static-access")
    @Test
    public void testLocalOptimization() {

        drs1.clear();
        drs2.clear();

        CoreElement ce4 = CoreManager.getCore(4);
        List<Implementation> ce4Impls = ce4.getImplementations();

        FakeAllocatableAction external10 = new FakeAllocatableAction(fao, 13, 0, ce4Impls);
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).setExpectedEnd(10);
        ((FullGraphSchedulingInformation) external10.getSchedulingInfo()).scheduled();
        external10.selectExecution(drs2, (FakeImplementation) external10.getImplementations()[0]);

        FakeAllocatableAction external20 = new FakeAllocatableAction(fao, 14, 0, ce4Impls);
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).setExpectedEnd(20);
        ((FullGraphSchedulingInformation) external20.getSchedulingInfo()).scheduled();
        external20.selectExecution(drs2, (FakeImplementation) external20.getImplementations()[0]);

        FakeAllocatableAction external90 = new FakeAllocatableAction(fao, 15, 0, ce4Impls);
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).setExpectedEnd(90);
        ((FullGraphSchedulingInformation) external90.getSchedulingInfo()).scheduled();
        external90.selectExecution(drs2, (FakeImplementation) external90.getImplementations()[0]);

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, ce4Impls);
        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        drs1.scheduleAction(action1);
        try {
            action1.tryToLaunch();
        } catch (Exception e) {
            // Nothing to do
        }

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, ce4Impls);
        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        drs1.scheduleAction(action2);
        try {
            action2.tryToLaunch();
        } catch (Exception e) {
            // Nothing to do
        }

        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 1, ce4Impls);
        action3.addDataPredecessor(external90);
        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        drs1.scheduleAction(action3);

        CoreElement ce5 = CoreManager.getCore(5);
        List<Implementation> ce5Impls = ce5.getImplementations();

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, ce5Impls);
        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        drs1.scheduleAction(action4);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 1, ce4Impls);
        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        drs1.scheduleAction(action5);

        CoreElement ce6 = CoreManager.getCore(6);
        List<Implementation> ce6Impls = ce6.getImplementations();

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 1, ce6Impls);
        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        drs1.scheduleAction(action6);

        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, ce5Impls);
        action7.addDataPredecessor(external10);
        action7.selectExecution(drs1, (FakeImplementation) action7.getImplementations()[0]);
        drs1.scheduleAction(action7);

        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, ce5Impls);
        action8.addDataPredecessor(external20);
        action8.selectExecution(drs1, (FakeImplementation) action8.getImplementations()[0]);
        drs1.scheduleAction(action8);

        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, ce4Impls);
        action9.addDataPredecessor(external90);
        action9.selectExecution(drs1, (FakeImplementation) action9.getImplementations()[0]);
        drs1.scheduleAction(action9);

        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, ce4Impls);
        action10.addDataPredecessor(action5);
        action10.selectExecution(drs1, (FakeImplementation) action10.getImplementations()[0]);
        drs1.scheduleAction(action10);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, ce4Impls);
        action11.addDataPredecessor(action6);
        action11.selectExecution(drs1, (FakeImplementation) action11.getImplementations()[0]);
        drs1.scheduleAction(action11);

        FakeAllocatableAction action12 = new FakeAllocatableAction(fao, 12, 0, ce4Impls);
        action12.addDataPredecessor(action5);
        action12.addDataPredecessor(action6);
        action12.selectExecution(drs1, (FakeImplementation) action12.getImplementations()[0]);
        drs1.scheduleAction(action12);

        // Simulate Scan results
        final LinkedList<AllocatableAction> runningActions = new LinkedList<>();
        final PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<>(1, drs1.getReadyComparator());
        final PriorityActionSet selectableActions = new PriorityActionSet(ScheduleOptimizer.getSelectionComparator());

        final long updateId = System.currentTimeMillis();

        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).setExpectedStart(0);
        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).lock();
        runningActions.add(action1);

        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).setExpectedStart(0);
        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).lock();
        runningActions.add(action2);

        ((FullGraphSchedulingInformation) action3.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action3.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action3.getSchedulingInfo()).setExpectedStart(90);
        readyActions.offer(action3);

        ((FullGraphSchedulingInformation) action4.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action4.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action4.getSchedulingInfo()).setExpectedStart(0);
        selectableActions.offer(action4);

        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).setExpectedStart(0);
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).optimizingSuccessor(action12);
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).optimizingSuccessor(action10);
        selectableActions.offer(action5);

        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).setExpectedStart(0);
        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).optimizingSuccessor(action12);
        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).optimizingSuccessor(action11);
        selectableActions.offer(action6);

        ((FullGraphSchedulingInformation) action7.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action7.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action7.getSchedulingInfo()).setExpectedStart(10);
        readyActions.offer(action7);

        ((FullGraphSchedulingInformation) action8.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action8.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action8.getSchedulingInfo()).setExpectedStart(20);
        readyActions.offer(action8);

        ((FullGraphSchedulingInformation) action9.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action9.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action9.getSchedulingInfo()).setExpectedStart(90);
        readyActions.offer(action9);

        ((FullGraphSchedulingInformation) action10.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action10.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action10.getSchedulingInfo()).setExpectedStart(0);

        ((FullGraphSchedulingInformation) action11.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action11.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action11.getSchedulingInfo()).setExpectedStart(0);

        ((FullGraphSchedulingInformation) action12.getSchedulingInfo()).setOnOptimization(true);
        ((FullGraphSchedulingInformation) action12.getSchedulingInfo()).setToReschedule(true);
        ((FullGraphSchedulingInformation) action12.getSchedulingInfo()).setExpectedStart(0);

        PriorityQueue<AllocatableAction> donationActions = new PriorityQueue<>(1,
                ScheduleOptimizer.getDonationComparator());

        drs1.rescheduleTasks(updateId, readyActions, selectableActions, runningActions, donationActions);
    }

    // @Test
    /**
     * Tests the actions without data dependencies.
     * 
     * @throws BlockedActionException When the action is blocked.
     * @throws UnassignedActionException When the action cannot be assigned.
     * @throws InvalidSchedulingException When the action has an invalid scheduling state.
     * @throws InterruptedException When the thread gets interrupted.
     */
    @SuppressWarnings("unchecked")
    public void testNoDataDependencies()
            throws BlockedActionException, UnassignedActionException, InvalidSchedulingException, InterruptedException {

        // Build graph
        /*
         * 1 --> 3 --> 5 -->6 --> 8 -->9 ----->11 -->12 --> 13 2 --> 4 ┘ └->7 ┘ └->10 ---| └-----┘ | |
         * ------------------------------------------------------- 14┘ 15┘
         */
        drs1.clear();

        CoreElement ce0 = CoreManager.getCore(0);
        List<Implementation> ce0Impls = ce0.getImplementations();
        CoreElement ce1 = CoreManager.getCore(1);
        List<Implementation> ce1Impls = ce1.getImplementations();
        CoreElement ce2 = CoreManager.getCore(2);
        List<Implementation> ce2Impls = ce2.getImplementations();
        CoreElement ce3 = CoreManager.getCore(3);
        List<Implementation> ce3Impls = ce3.getImplementations();

        final FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, ce0Impls);
        final FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, ce0Impls);
        final FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 0, ce0Impls);
        final FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, ce0Impls);
        final FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 0, ce1Impls);
        final FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 0, ce0Impls);
        final FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, ce2Impls);
        final FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, ce3Impls);
        final FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, ce0Impls);
        final FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, ce2Impls);
        final FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, ce3Impls);
        final FakeAllocatableAction action12 = new FakeAllocatableAction(fao, 12, 0, ce0Impls);
        final FakeAllocatableAction action13 = new FakeAllocatableAction(fao, 13, 0, ce1Impls);

        FakeAllocatableAction action14 = new FakeAllocatableAction(fao, 14, 0, ce0Impls);
        action14.selectExecution(drs2, (FakeImplementation) action14.getImplementations()[0]);
        FullGraphSchedulingInformation dsi14 = (FullGraphSchedulingInformation) action14.getSchedulingInfo();
        dsi14.setExpectedEnd(10_000);

        FakeAllocatableAction action15 = new FakeAllocatableAction(fao, 15, 0, ce0Impls);
        action15.selectExecution(drs2, (FakeImplementation) action15.getImplementations()[0]);
        FullGraphSchedulingInformation dsi15 = (FullGraphSchedulingInformation) action15.getSchedulingInfo();
        dsi15.setExpectedEnd(12_000);

        action1.selectExecution(drs1, (FakeImplementation) action1.getImplementations()[0]);
        action1.tryToLaunch();

        action2.selectExecution(drs1, (FakeImplementation) action2.getImplementations()[0]);
        action2.tryToLaunch();

        action3.selectExecution(drs1, (FakeImplementation) action3.getImplementations()[0]);
        addSchedulingDependency(action1, action3);

        action4.selectExecution(drs1, (FakeImplementation) action4.getImplementations()[0]);
        addSchedulingDependency(action2, action4);

        action5.selectExecution(drs1, (FakeImplementation) action5.getImplementations()[0]);
        action5.addDataPredecessor(action2);
        addSchedulingDependency(action3, action5);
        addSchedulingDependency(action4, action5);

        action6.selectExecution(drs1, (FakeImplementation) action6.getImplementations()[0]);
        action6.addDataPredecessor(action2);
        addSchedulingDependency(action5, action6);

        action7.selectExecution(drs1, (FakeImplementation) action7.getImplementations()[0]);
        action7.addDataPredecessor(action2);
        addSchedulingDependency(action5, action7);

        action8.selectExecution(drs1, (FakeImplementation) action8.getImplementations()[0]);
        action8.addDataPredecessor(action5);
        addSchedulingDependency(action6, action8);
        addSchedulingDependency(action7, action8);

        action9.selectExecution(drs1, (FakeImplementation) action9.getImplementations()[0]);
        addSchedulingDependency(action8, action9);
        action9.addDataPredecessor(action5);

        action10.selectExecution(drs1, (FakeImplementation) action10.getImplementations()[0]);
        addSchedulingDependency(action8, action10);

        action11.selectExecution(drs1, (FakeImplementation) action11.getImplementations()[0]);
        addSchedulingDependency(action9, action11);
        addSchedulingDependency(action10, action11);
        action11.addDataPredecessor(action14);

        action12.selectExecution(drs1, (FakeImplementation) action12.getImplementations()[0]);
        addSchedulingDependency(action11, action12);

        action13.selectExecution(drs1, (FakeImplementation) action13.getImplementations()[0]);
        addSchedulingDependency(action11, action13);
        addSchedulingDependency(action12, action13);
        action13.addDataPredecessor(action15);

        // debugActions(action1, action2, action3, action4, action5, action6, action7, action8, action9, action10,
        // action11, action12, action13 );
        LinkedList<AllocatableAction>[] actions = new LinkedList[CoreManager.getCoreCount()];
        for (int i = 0; i < actions.length; i++) {
            actions[i] = new LinkedList<>();
        }

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<>(1,
                FullGraphResourceScheduler.getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet selectableActions = new PriorityActionSet(FullGraphResourceScheduler.getScanComparator());

        final LinkedList<AllocatableAction> runningActions = drs1.scanActions(readyActions, selectableActions);

        HashMap<AllocatableAction, Long> expectedReady = new HashMap<>();
        expectedReady.put(action11, 10_000L);
        expectedReady.put(action13, 12_000L);
        Verifiers.verifyReadyActions(new PriorityQueue<>(readyActions), expectedReady);
        AllocatableAction[] expectedSelectable = new AllocatableAction[] { action3, action4, action10, action12 };
        Verifiers.verifyPriorityActions(new PriorityActionSet(selectableActions), expectedSelectable);

        PriorityQueue<AllocatableAction> donationActions = new PriorityQueue<>(1,
                ScheduleOptimizer.getDonationComparator());
        drs1.rescheduleTasks(System.currentTimeMillis(), readyActions, selectableActions, runningActions,
                donationActions);

        /*
         * drs.seekGaps(System.currentTimeMillis(), gaps, actions);
         * 
         * long[][][] times = { new long[][]{//CORE 0 new long[]{0, CORE0}, //1 new long[]{0, CORE0}, //2 new
         * long[]{CORE0, 2 * CORE0}, //3 new long[]{CORE0, 2 * CORE0}, //4 new long[]{2 * CORE0 + CORE1, 3 * CORE0 +
         * CORE1}, //6 new long[]{3 * CORE0 + CORE1 + CORE3, 4 * CORE0 + CORE1 + CORE3}, //9 new long[]{10_000 + CORE3,
         * 10_000 + CORE3 + CORE0}, //12 }, new long[][]{//CORE 1 new long[]{2 * CORE0, 2 * CORE0 + CORE1}, //5 new
         * long[]{12_000, 12_000 + CORE1}, //13 }, new long[][]{//CORE 2 new long[]{2 * CORE0 + CORE1, 2 * CORE0 + CORE1
         * + CORE2}, //7 new long[]{3 * CORE0 + CORE1 + CORE3, 3 * CORE0 + CORE1 + CORE2 + CORE3}, //10 }, new
         * long[][]{//CORE 3 new long[]{3 * CORE0 + CORE1, 3 * CORE0 + CORE1 + CORE3}, //8 new long[]{10_000, 10_000 +
         * CORE3}, //11 },}; Verifiers.verifyUpdate(actions, times);
         * 
         * Gap[] expectedGaps = { new Gap(2 * CORE0, 3 * CORE0 + CORE1, action3, new FakeResourceDescription(1), 0), new
         * Gap(2 * CORE0 + CORE1 + CORE2, 3 * CORE0 + CORE1, action7, new FakeResourceDescription(1), 0), new Gap(3 *
         * CORE0 + CORE1 + CORE3, 10_000, action8, new FakeResourceDescription(1), 0), new Gap(3 * CORE0 + CORE1 + CORE2
         * + CORE3, 10_000, action10, new FakeResourceDescription(1), 0), new Gap(4 * CORE0 + CORE1 + CORE3, 10_000,
         * action9, new FakeResourceDescription(2), 0), new Gap(10_000 + CORE3 + CORE0, 12_000, action12, new
         * FakeResourceDescription(2), 0), new Gap(10_000 + CORE3, 12_000, action11, new FakeResourceDescription(1), 0),
         * new Gap(10_000 + CORE3, Long.MAX_VALUE, action11, new FakeResourceDescription(1), 0), new Gap(12_000 + CORE1,
         * Long.MAX_VALUE, action13, new FakeResourceDescription(3), 0),}; Verifiers.verifyGaps(gaps, expectedGaps);
         */
    }

    private void addSchedulingDependency(FakeAllocatableAction pred, FakeAllocatableAction succ) {
        FullGraphSchedulingInformation predDSI = (FullGraphSchedulingInformation) pred.getSchedulingInfo();
        predDSI.lock();
        FullGraphSchedulingInformation succDSI = (FullGraphSchedulingInformation) succ.getSchedulingInfo();
        succDSI.lock();
        if (pred.isPending()) {
            predDSI.addSuccessor(succ);
            succDSI.addPredecessor(pred);
        }
    }

}
