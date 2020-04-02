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

package es.bsc.compss.scheduler.heuristics;

import es.bsc.compss.util.ErrorManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;


public class LNSNL {

    // DAG containing C, z and succ
    private DAG dag;

    // Resources contains ibw, mfs, h, overhead representing the resources used in the system
    private Resources resources;

    // number of tasks
    private int n;

    // number of resources
    private int m;

    private int dummyNodes;

    private ArrayList<Boolean> dummies;


    public LNSNL(List<String> workers) {
        HeuristicsConfiguration.load();
        readInput(workers);
    }

    public LNSNL(DAG dag, Resources resources, int n, int m) {
        this.dag = dag;
        this.resources = resources;
        this.n = n;
        this.m = m;
    }

    private void readInput(List<String> workers) {
        try (
            BufferedReader br = new BufferedReader(new FileReader(new File(HeuristicsConfiguration.HEURISTICS_FILE)))) {
            String lines;
            br.readLine();
            lines = br.readLine();
            String[] st = lines.trim().split("\\s+");
            this.n = Integer.parseInt(st[2].substring(0, st[2].indexOf(';')));
            br.readLine();
            br.readLine();
            br.readLine();
            boolean[][] succ = new boolean[n][n];
            for (int i = 0; i < n; ++i) {
                lines = br.readLine();
                st = lines.trim().split("\\s+");
                for (int j = 0; j < n; ++j) {
                    if (j == 0) {
                        int pos = st[0].indexOf('[');
                        st[0] = st[0].substring(pos + 1, pos + 2);
                    }
                    succ[i][j] = Integer.parseInt(st[j]) == 1;
                }
            }
            br.readLine();
            br.readLine();
            float[][] z = new float[n][n];
            for (int i = 0; i < n; ++i) {
                lines = br.readLine();
                st = lines.trim().split("\\s+");
                for (int j = 0; j < n; ++j) {
                    if (j == 0) {
                        int pos = st[0].indexOf('[');
                        st[0] = st[0].substring(pos + 1);
                    }
                    z[i][j] = Float.parseFloat(st[j]);
                }
            }
            br.readLine();
            lines = br.readLine();
            st = lines.trim().split("\\s+");
            this.m = Integer.parseInt(st[2].substring(0, st[2].indexOf(';')));
            br.readLine();
            float[][] ibw = new float[m][m];
            for (int i = 0; i < m; ++i) {
                lines = br.readLine();
                st = lines.trim().split("\\s+");
                for (int j = 0; j < m; ++j) {
                    if (j == 0) {
                        int pos = st[0].indexOf('[');
                        st[0] = st[0].substring(pos + 1);
                    }
                    ibw[i][j] = Float.parseFloat(st[j]);
                }
            }
            br.readLine();
            br.readLine();
            lines = br.readLine();
            st = lines.trim().split("\\s+");
            float mfs = Float.parseFloat(st[2].substring(0, st[2].indexOf(';')));
            lines = br.readLine();
            st = lines.trim().split("\\s+");
            float h = Float.parseFloat(st[2].substring(0, st[2].indexOf(';')));
            br.readLine();
            float[][] C = new float[n][m];
            for (int i = 0; i < n; ++i) {
                lines = br.readLine();
                st = lines.trim().split("\\s+");
                for (int j = 0; j < m; ++j) {
                    if (j == 0) {
                        int pos = st[0].indexOf('[');
                        st[0] = st[0].substring(pos + 1);
                    }
                    C[i][j] = Float.parseFloat(st[j]);
                }
            }
            br.readLine();
            lines = br.readLine();
            st = lines.trim().split("\\s+");
            this.dummies = new ArrayList<>(n);
            for (int j = 0; j < n; j++) {
                if (j == 0) {
                    int pos = st[j + 2].indexOf('[');
                    if (st[j + 2].substring(pos + 1).charAt(0) == '0') {
                        this.dummies.add(false);
                    } else {
                        this.dummies.add(true);
                        this.dummyNodes++;
                    }
                } else {
                    if (st[j + 2].charAt(0) == '0') {
                        this.dummies.add(false);
                    } else {
                        this.dummies.add(true);
                        this.dummyNodes++;
                    }
                }
            }

            lines = br.readLine();
            st = lines.trim().split("\\s+");
            float overhead = Float.parseFloat(st[2].substring(0, st[2].indexOf(';')));

            ArrayList<String> workersOrder = new ArrayList<>(m);
            lines = br.readLine();
            st = lines.trim().split("\\s+");
            for (int j = 2; j < st.length - 1; j++) {
                if (j == 2) {
                    int pos = st[j].indexOf('[');
                    workersOrder.add(st[j].substring(pos + 1));
                } else {
                    workersOrder.add(st[j]);
                }
            }

            this.resources = new Resources(ibw, mfs, h, overhead, m);
            resources.setResourceNames(workers);

            HashMap<String, float[]> mapC = new HashMap<>();
            for (int j = 0; j < m; ++j) {
                float[] times = new float[n];
                for (int i = 0; i < n; ++i) {
                    times[i] = C[i][j];
                }
                // mapC.put(workers.get(j), times);
                mapC.put(workersOrder.get(j), times);
            }
            this.dag = new DAG(mapC, z, succ, n);
        } catch (IOException | StringIndexOutOfBoundsException | ArrayIndexOutOfBoundsException
            | NullPointerException e) {
            ErrorManager.fatal("ERROR WHILE PARSING INPUT FILE. FILE FORMAT NOT CORRECT");
        }
    }

    public void setSafetyMargin(float safetyMargin) {
        this.dag.setSafetyMargin(safetyMargin);
    }

    public void removeResource(String name) {
        this.resources.removeResource(name);
        this.dag.removeResource(name);
        this.m--;
    }

    public void addResource(String name) {
        this.resources.addResource(name);
        this.dag.addResource(name);
        this.m++;
    }

    public int getNumTasks() {
        return dag.getN() - this.dummyNodes;
    }

    private int computeSuccessors(int taskId) {
        int nSucc = 0;
        boolean[][] succ = this.dag.getSucc();
        for (int i = 0; i < this.n; ++i) {
            if (succ[taskId][i]) {
                ++nSucc;
            }
        }
        return nSucc;
    }

    public ReadyQueueElement getNextReadyTask(PriorityQueue<ReadyQueueElement> readyQueue) {
        return readyQueue.poll();
    }

    // TODO: move it to a common folder/class inside mapTaskToResource
    private ResourceTransferCost selectBestResource(Resources resources, ArrayList<Float> workload, DAG dag,
        ReadyQueueElement rqe, ArrayList<Integer> computingNode, ArrayList<Float> endTime) {
        int resource = 0;
        ArrayList<Float> overheads = new ArrayList<>(Collections.nCopies(this.m, 0.0f));
        ArrayList<Float> transfers = new ArrayList<>(Collections.nCopies(this.m, 0.0f));
        ArrayList<Float> completionTime = new ArrayList<>(Collections.nCopies(this.m, 0.0f));
        ArrayList<Float> releaseTimes = new ArrayList<>(Collections.nCopies(this.m, rqe.getReleaseTime()));
        boolean[][] succ = dag.getSucc();
        float[][] ibw = resources.getIBW();
        float[][] z = dag.getZ();
        Map<String, float[]> C = dag.getC();
        float transfer;
        float overhead;
        for (int idxRes = 0; idxRes < this.m; ++idxRes) {
            float maxTime = 0.0f;
            for (int pred = 0; pred < rqe.getId(); ++pred) {
                if (succ[pred][rqe.getId()]) {
                    float transferTime = (float) (ibw[computingNode.get(pred)][idxRes]
                        * (Math.ceil((z[pred][rqe.getId()] / resources.getMFS())) * resources.getH()
                            + z[pred][rqe.getId()]));

                    float t = endTime.get(pred);// + transferTime;// + resources.getOverhead();
                    if (computingNode.get(pred) != idxRes
                        && workload.get(idxRes) <= t + resources.getOverhead() + transferTime) {
                        if (t + resources.getOverhead() + transferTime > maxTime) {
                            releaseTimes.set(idxRes, endTime.get(pred));
                            transfers.set(idxRes, transferTime);
                            maxTime = t + resources.getOverhead() + transferTime;
                        }
                        overheads.set(idxRes, resources.getOverhead());
                    } else {
                        if (computingNode.get(pred) == idxRes) {
                            if (maxTime < endTime.get(pred)) {
                                transfers.set(idxRes, transferTime);
                                overheads.set(idxRes, 0.0f);
                            }
                        }
                    }
                }
            }
            float sum = C.get(this.resources.getWorker(idxRes))[rqe.getId()] + transfers.get(idxRes);
            if (workload.get(idxRes) > releaseTimes.get(idxRes)) {
                completionTime.set(idxRes, (sum + workload.get(idxRes)));
            } else {
                completionTime.set(idxRes, (sum + releaseTimes.get(idxRes)));
            }
        }

        float min = completionTime.get(0);
        for (int i = 1; i < this.m; ++i) {
            if (completionTime.get(i) < min) {
                min = completionTime.get(i);
                resource = i;
            } else if (completionTime.get(i) == min) {
                if (workload.get(resource) > workload.get(i)) {
                    resource = i;
                }
            }
        }

        overhead = overheads.get(resource);
        transfer = transfers.get(resource);
        rqe.setReleaseTime(releaseTimes.get(resource));
        return new ResourceTransferCost(resource, overhead, transfer);
    }

    // TODO: move to a common class
    public ResourceTransferCost mapTaskToResource(ReadyQueueElement rqe, Resources resources, DAG dag,
        ArrayList<Float> workload, ArrayList<Integer> computingNode, ArrayList<Float> endTime) {
        int taskId = rqe.getId();
        ResourceTransferCost rtf;
        if (checkDummy(taskId)) {
            rtf = checkDummyTransferTime(dag, resources, workload, rqe, computingNode, endTime);
        } else {
            rtf = selectBestResource(resources, workload, dag, rqe, computingNode, endTime);
        }

        float releaseTime = rqe.getReleaseTime();
        if (releaseTime + rtf.getTransfer() + rtf.getOverhead() < workload.get(rtf.getResource())) {
            rqe.setReleaseTime(workload.get(rtf.getResource()));
        } else {
            rqe.setReleaseTime(releaseTime + rtf.getTransfer() + rtf.getOverhead());
        }
        return rtf;
    }

    private ResourceTransferCost checkDummyTransferTime(DAG dag, Resources resources, ArrayList<Float> workload,
        ReadyQueueElement rqe, ArrayList<Integer> computingNode, ArrayList<Float> endTime) {

        boolean[][] succ = dag.getSucc();
        float[][] ibw = resources.getIBW();
        float[][] z = dag.getZ();
        float overhead = 0;
        float transfer = 0;
        int resource = 0;
        float maxTime = 0.0f;
        for (int pred = 0; pred < rqe.getId(); ++pred) {
            if (succ[pred][rqe.getId()]) {
                float transferTime = (float) (ibw[computingNode.get(pred)][0]
                    * (Math.ceil((z[pred][rqe.getId()] / resources.getMFS())) * resources.getH()
                        + z[pred][rqe.getId()]));
                float t = endTime.get(pred);// + transferTime;// + resources.getOverhead();
                if (computingNode.get(pred) != resource
                    && workload.get(0) <= t + resources.getOverhead() + transferTime) {
                    if (t + resources.getOverhead() + transferTime > maxTime) {
                        rqe.setReleaseTime(endTime.get(pred));
                        transfer = transferTime;
                        maxTime = t + resources.getOverhead() + transferTime;
                    }
                    overhead = resources.getOverhead();
                } else {
                    if (computingNode.get(pred) == resource) {
                        if (maxTime < endTime.get(pred)) {
                            transfer = transferTime;
                            overhead = 0;
                        }
                    }
                }
            }
        }
        return new ResourceTransferCost(resource, overhead, transfer);
    }

    private boolean checkDummy(int readyTask) {
        float sum = 0.0f;
        HashMap<String, float[]> C = this.dag.getC();
        for (String worker : C.keySet()) {
            sum += C.get(worker)[readyTask];
        }
        return (sum == 0.0f);
    }

    // TODO: move it to a common class
    public void releaseTasks(PriorityQueue readyQueue, DAG dag, int readyTask, ArrayList<Boolean> completed,
        ArrayList<Float> endTime) {
        boolean[][] succ = dag.getSucc();
        boolean ready;
        for (int s = readyTask + 1; s < this.n; ++s) {
            if (succ[readyTask][s]) {
                ready = true;
                float releaseTime = 0;
                for (int pred = 0; (pred < s) && ready; ++pred) {
                    if (succ[pred][s]) {
                        if (!completed.get(pred)) {
                            ready = false;
                        }
                        if (releaseTime < endTime.get(pred)) {
                            releaseTime = endTime.get(pred);
                        }
                    }
                }
                if (ready) {
                    int nSucc = computeSuccessors(s);
                    ReadyQueueElement rqe = new ReadyQueueElement(s, releaseTime, nSucc);
                    rqe.addToQueue(readyQueue);
                }
            }
        }
    }

    public Result schedule() {

        ArrayList<Boolean> completed = new ArrayList<>(Collections.nCopies(this.n, false));
        ArrayList<Float> iniTime = new ArrayList<>(Collections.nCopies(this.n, 0.0f));
        ArrayList<Float> endTime = new ArrayList<>(Collections.nCopies(this.n, 0.0f));
        ArrayList<Integer> computingNode = new ArrayList<>(Collections.nCopies(this.n, 0));
        ArrayList<Float> workload = new ArrayList<>(Collections.nCopies(this.m, 0.0f)); // end times for each worker
        ArrayList<Integer> dummies = new ArrayList<>();

        int sourceId = 0; // dummy source node

        int nSucc = computeSuccessors(sourceId);
        float releaseTime = 0;

        ReadyQueueElement st = new ReadyQueueElement(sourceId, releaseTime, nSucc);

        PriorityQueue readyQueue = new PriorityQueue(new ReadyQueueElementComparator());
        readyQueue.offer(st);
        int readyTask;
        int resource;
        boolean dummy;
        ResourceTransferCost rtf;
        while (!readyQueue.isEmpty()) {
            ReadyQueueElement rqe = getNextReadyTask(readyQueue);
            readyTask = rqe.getId();

            dummy = checkDummy(readyTask);
            if (dummy) {
                dummies.add(readyTask);
            }

            // System.out.println("Next ready task selected: " + (readyTask + 1));

            // for the selected ready task, the bestResource is computed based on the minCT
            rtf = mapTaskToResource(rqe, this.resources, this.dag, workload, computingNode, endTime);
            releaseTime = rqe.getReleaseTime();

            // System.out.println("Resource to execute the ready task selected: " + rtf.getResource());

            completed.set(readyTask, true);

            resource = rtf.getResource();

            float startTime = releaseTime;
            iniTime.set(readyTask, startTime);
            endTime.set(readyTask, (startTime + this.dag.getC(readyTask, this.resources.getWorker(resource))));
            computingNode.set(readyTask, resource);
            workload.set(resource, endTime.get(readyTask));
            // System.out.println("Task " + (readyTask + 1) + " starts at " + iniTime.get(readyTask) + " and ends at "
            // + endTime.get(readyTask));

            // update Q, release Tasks dependencies
            releaseTasks(readyQueue, this.dag, readyTask, completed, endTime);
        }

        float rub = 0;
        for (float w : workload) {
            if (w > rub) {
                rub = w;
            }
        }

        return updateStructures(iniTime, endTime, computingNode, rub);
    }

    private Result updateStructures(ArrayList<Float> iniTime, ArrayList<Float> endTime,
        ArrayList<Integer> computingNode, float rub) {
        int i = 0;
        int id = 0;
        // System.out.println("Mapping:");
        LinkedHashMap<Long, String> taskResource = new LinkedHashMap<>();
        for (int res : computingNode) {
            if (!checkDummy(i)) {
                // System.out.println("Task " + (i + 1) + " in resource " + res + " starts at " + iniTime.get(i)
                // + " and ends at " + endTime.get(i));
                // System.out.println((i + 1 - id) + " " + resources.getWorker(res));
                taskResource.put((long) (i + 1 - id), resources.getWorker(res));
            } else {
                id++;
            }
            ++i;
        }
        int r = 0;
        ComparePair comp = new ComparePair();
        LinkedHashMap<String, ArrayList<Integer>> map = new LinkedHashMap<>();
        int[] iters = new int[this.n - this.dummyNodes];
        while (r < resources.getM()) {
            ArrayList<Pair> order = new ArrayList<>();
            ArrayList<Integer> orderedIds = new ArrayList<>();
            // System.out.println("Resource " + r);
            i = 0;
            id = 0;
            for (int res : computingNode) {
                if (res == r) {
                    if (!checkDummy(i)) {
                        order.add(new Pair((i), iniTime.get(i)));
                    } else {
                        id++;
                    }
                }
                ++i;
            }
            comp.compare(order);
            int j = 0;
            for (Pair p : order) {
                // System.out.println(p.getTaskId());
                orderedIds.add(p.getTaskId());
                iters[p.getTaskId() - 1] = j;
                ++j;
            }
            map.put(resources.getWorker(r), orderedIds);
            ++r;
        }
        ArrayList<Float> eT = new ArrayList<>(this.n);
        for (i = 0; i < this.n; ++i) {
            if (!this.dummies.get(i)) {
                eT.add(endTime.get(i));
                // System.out.println(endTime.get(i));
            }
        }

        return new Result(taskResource, map, iters, rub, eT);
    }


    private class Pair {

        private int taskId;

        private float iniTime;


        public Pair(int taskId, float iniTime) {
            this.taskId = taskId;
            this.iniTime = iniTime;
        }

        public int getTaskId() {
            return this.taskId;
        }
    }

    private class ComparePair {

        public void compare(ArrayList<Pair> arr) {
            arr.sort((p1, p2) -> Float.compare(p1.iniTime, p2.iniTime));
        }
    }

    private class ReadyQueueElement {

        private int taskId;

        private float releaseTime;

        private int nSucc;


        public ReadyQueueElement(int taskId, float releaseTime, int nSucc) {
            this.taskId = taskId;
            this.releaseTime = releaseTime;
            this.nSucc = nSucc;
        }

        public int getId() {
            return this.taskId;
        }

        public float getReleaseTime() {
            return this.releaseTime;
        }

        public void setReleaseTime(float releaseTime) {
            this.releaseTime = releaseTime;
        }

        public int getSucc() {
            return this.nSucc;
        }

        public void addToQueue(PriorityQueue<ReadyQueueElement> queue) {
            queue.offer(this);
        }

    }

    private class ReadyQueueElementComparator implements Comparator<ReadyQueueElement> {

        @Override
        public int compare(ReadyQueueElement st1, ReadyQueueElement st2) {
            int firstComparison = -Integer.compare(st1.getSucc(), st2.getSucc());
            if (firstComparison == 0) {
                return Integer.compare(st1.taskId, st2.taskId);
            } else {
                return firstComparison;
            }
        }
    }

    private class ResourceTransferCost {

        private int resource;

        private float overhead;

        private float transfer;


        public ResourceTransferCost(int resource, float overhead, float transfer) {
            this.resource = resource;
            this.overhead = overhead;
            this.transfer = transfer;
        }

        public int getResource() {
            return this.resource;
        }

        public float getTransfer() {
            return this.transfer;
        }

        public float getOverhead() {
            return this.overhead;
        }
    }
}