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

import java.util.ArrayList;
import java.util.LinkedHashMap;


public class Result {

    private LinkedHashMap<Long, String> taskResource;

    private LinkedHashMap<String, ArrayList<Integer>> map;

    private int[] iters;

    private float rub;

    private ArrayList<Float> endTime;


    /**
     * Constructs a Result object that will be returned to update the scheduler internal structures.
     */
    public Result(LinkedHashMap<Long, String> taskResource, LinkedHashMap<String, ArrayList<Integer>> map, int[] iters,
        float rub, ArrayList<Float> endTime) {
        this.taskResource = taskResource;
        this.map = map;
        this.iters = iters;
        this.rub = rub;
        this.endTime = endTime;
    }

    public LinkedHashMap<Long, String> getTaskResource() {
        return this.taskResource;
    }

    public LinkedHashMap<String, ArrayList<Integer>> getOrderOfTasks() {
        return this.map;
    }

    public int[] getIters() {
        return this.iters;
    }

    public ArrayList<Float> getEndTime() {
        return this.endTime;
    }

    public float getRub() {
        return this.rub;
    }
}