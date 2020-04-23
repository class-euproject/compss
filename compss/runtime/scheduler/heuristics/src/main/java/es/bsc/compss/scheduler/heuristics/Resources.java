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

import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;


public class Resources {

    // ibw matrix that shows the ibw from a given resource k to another l
    private float[][] ibw;

    // maxmimum frame size allowed
    private float mfs;

    // size of header
    private float h;

    // safety margin for network calculation
    private float overhead;

    // number of resources
    private int m;

    private ArrayList<String> resourceNames;

    private ArrayList<String> originalWorkers;

    private LinkedHashMap<String, Integer> posByName;

    private float bw;


    public Resources(float[][] ibw, float mfs, float h, float overhead, int m) {
        this.ibw = ibw;
        this.mfs = mfs;
        this.h = h;
        this.overhead = overhead;
        this.m = m;
        this.resourceNames = new ArrayList<>(this.m);
        this.originalWorkers = new ArrayList<>(this.m);
        this.posByName = new LinkedHashMap<>();

        for (int i = 0; i < this.m; ++i) {
            for (int j = 0; j < this.m; ++j) {
                if (this.ibw[i][j] != 0) {
                    this.bw = this.ibw[i][j];
                    break;
                }
            }
        }
    }

    public Resources(Resources resources) {
        this.ibw = resources.ibw;
        this.mfs = resources.mfs;
        this.h = resources.h;
        this.overhead = resources.overhead;
        this.m = resources.m;
        this.resourceNames = new ArrayList<>(m);
        this.posByName = new LinkedHashMap<>();
    }

    public float[][] getIBW() {
        return this.ibw;
    }

    public float getMFS() {
        return this.mfs;
    }

    public void setMFS(float mfs) {
        this.mfs = mfs;
    }

    public float getH() {
        return this.h;
    }

    public void setH(float h) {
        this.h = h;
    }

    public float getOverhead() {
        return this.overhead;
    }

    public void setOverhead(float overhead) {
        this.overhead = overhead;
    }

    public int getM() {
        return this.m;
    }

    public void setM(int m) {
        this.m = m;
    }

    // public void removeResource(String name, int pos) {
    public int removeResource(String name) {
        this.m--;
        int M = this.m;
        float[][] newIBW = new float[M][M];
        for (int i = 0; i < M; ++i) {
            System.arraycopy(ibw[i], 0, newIBW[i], 0, M);
        }
        this.ibw = newIBW;

        int i = 0;
        boolean found = false;
        int pos = this.posByName.get(name);
        for (String worker : this.resourceNames) {
            if (worker.equals(name)) {
                // this.resourceNames.set(i, null);
                // this.resourceNames.remove(i);
                found = true;
                break;
            }
            ++i;
        }
        if (found) {
            this.resourceNames.remove(i);
        }

        return pos;
    }

    /*
     * private float findBw() { for (int i = 0; i < this.m - 1; ++i) { for (int j = 0; j < this.m - 1; ++j) { if
     * (this.ibw[i][j] != 0) { return this.ibw[i][j]; } } } return 0.0f; }
     */

    /**
     * Adds a new resource to the resource pool to be considered by the scheduler.
     *
     * @param name Name of the resource to add.
     */
    public int addResource(String name) {
        this.m++;
        int newM = this.m;
        float[][] newIBW = new float[newM][newM];
        // System.out.println("SIZE OF IBW " + ibw.length);
        // float bw = findBw();
        for (int i = 0; i < newM; ++i) {
            for (int j = 0; j < newM; ++j) {
                if (i != j) {
                    newIBW[i][j] = this.bw;
                }
            }
        }
        this.ibw = newIBW;

        int pos = this.posByName.get(name);
        int i = 0;
        for (String worker : this.originalWorkers) {
            // System.out.println(worker);
            if (worker.equals(name)) {
                // this.resourceNames.set(i, worker);
                this.resourceNames.add(i, worker);
            }
            ++i;
        }
        return pos;
    }

    /**
     * Adds a new resource to the resource pool to be considered by the scheduler.
     *
     * @param name Name of the resource to add.
     */
    public int addResourceCloud(String cloudProvider, String name) {
        this.m++;
        int newM = this.m;
        float[][] newIBW = new float[newM][newM];
        for (int i = 0; i < newM; ++i) {
            for (int j = 0; j < newM; ++j) {
                if (i != j) {
                    newIBW[i][j] = this.bw;
                }
            }
        }
        this.ibw = newIBW;

        System.out.println("CLOUD PROVIDER " + cloudProvider);
        for (String nameW : posByName.keySet()) {
            System.out.println("POS BY BAME " + nameW);
        }

        int pos = this.posByName.get(cloudProvider);
        int i = 0;
        for (String worker : this.originalWorkers) {
            // System.out.println(worker);
            if (worker.equals(cloudProvider)) {
                // this.resourceNames.set(i, worker);
                this.resourceNames.add(i, name);
            }
            ++i;
        }
        return pos;
    }

    /**
     * Sets the internal structure that represents the names of the workers to identify them.
     *
     * @param names Array containing the names.
     */
    public void setResourceNames(List<String> names) {
        this.resourceNames = new ArrayList<>(names);
        this.originalWorkers = new ArrayList<>(names);
        for (String name : names) {
            int pos = this.posByName.size();
            this.posByName.put(name, pos);
            System.out.println("HAHAHA " + name);
        }
    }

    public String getWorker(int pos) {
        return this.resourceNames.get(pos);
    }
}