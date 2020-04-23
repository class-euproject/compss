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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DAG {

    // float matrix containing the execution times of each task for each resource
    private HashMap<String, float[]> C;

    // float matrix containing the data size transfers between tasks
    private float[][] z;

    // boolean matrix indicating the precedence relationships between tasks
    private boolean[][] succ;

    private int n;

    private Map<String, float[]> origC;

    private float safetyMargin;

    private List<String> workerOrder;


    public DAG(Map<String, float[]> C, float[][] z, boolean[][] succ, int n, List<Boolean> isCloud,
        List<String> workersOrder) {
        this.C = new HashMap<>();
        this.origC = new HashMap<>();
        for (String worker : C.keySet()) {
            this.C.put(worker, Arrays.copyOf(C.get(worker), n));
            this.origC.put(worker, Arrays.copyOf(C.get(worker), n));
        }

        for (int i = 0; i < isCloud.size(); i++) {
            if (isCloud.get(i)) {
                this.C.remove(workersOrder.get(i));
            }
        }

        this.z = Arrays.copyOf(z, z.length);
        this.succ = Arrays.copyOf(succ, succ.length);
        this.n = n;
        this.safetyMargin = 0.0f; // TODO: update it with the correct safetyMargin used
    }

    public HashMap<String, float[]> getC() {
        return this.C;
    }

    public float getC(int task, String res) {
        return this.C.get(res)[task];
    }

    public float[][] getZ() {
        return this.z;
    }

    public float getZ(int pred, int succ) {
        return this.z[pred][succ];
    }

    public void setZ(float[][] z) {
        this.z = z;
    }

    public boolean[][] getSucc() {
        return this.succ;
    }

    public void setSucc(boolean[][] succ) {
        this.succ = succ;
    }

    public int getN() {
        return this.n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public void removeResource(String worker) {
        this.C.remove(worker);
    }

    public void addResource(String worker) {
        float[] tmp = Arrays.copyOf(this.origC.get(worker), this.n);
        for (int i = 0; i < this.n; ++i) {
            tmp[i] += tmp[i] * this.safetyMargin;
        }
        this.C.put(worker, tmp);
    }

    public void addResourceCloud(String cloudProvider, String worker) {
        float[] tmp = Arrays.copyOf(this.origC.get(cloudProvider), this.n);
        for (int i = 0; i < this.n; ++i) {
            tmp[i] += tmp[i] * this.safetyMargin;
        }
        this.C.put(worker, tmp);
    }

    public void printDAG() {
        System.out.println("SUCC:");
        for (int i = 0; i < this.n; ++i) {
            for (int j = 0; j < this.n; ++j) {
                System.out.print(this.succ[i][j] + " ");
            }
            System.out.println();
        }

        System.out.println("Z:");
        for (int i = 0; i < this.n; ++i) {
            for (int j = 0; j < this.n; ++j) {
                System.out.print(this.z[i][j] + " ");
            }
            System.out.println();
        }

        System.out.println("C:");
        for (String worker : this.C.keySet()) {
            float[] times = this.C.get(worker);
            System.out.print("Worker " + worker + ": ");
            for (float time : times) {
                System.out.print(time + " ");
            }
            System.out.println();
        }
    }

    public float getSafetyMargin() {
        return this.safetyMargin;
    }

    public void setSafetyMargin(float safetyMargin) {
        this.safetyMargin = safetyMargin;
        for (String worker : this.C.keySet()) {
            float[] times = this.C.get(worker);
            for (int j = 0; j < this.n; ++j) {
                times[j] = this.origC.get(worker)[j] + (this.origC.get(worker)[j] * this.safetyMargin);
            }
            this.C.put(worker, times);
        }
    }
}