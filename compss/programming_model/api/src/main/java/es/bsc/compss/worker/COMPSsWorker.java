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
package es.bsc.compss.worker;

import java.util.HashMap;
import java.util.Map;


public class COMPSsWorker {

    public static final String COMPSS_TASK_ID = "COMPSS_TASK_ID";

    private static final Map<Integer, Boolean> TASKS_TO_CANCEL = new HashMap<>();


    /**
     * Add a cancellation point.
     * 
     * @throws Exception While waiting for the cancellation point.
     */
    public static final void cancellationPoint() throws Exception {
        String taskIdStr = System.getProperty(COMPSS_TASK_ID);
        if (taskIdStr != null) {
            Boolean toCancel = TASKS_TO_CANCEL.get(Integer.parseInt(taskIdStr));
            if (toCancel != null && toCancel) {
                throw new Exception("Task " + taskIdStr + " has been cancelled.");
            }
        }
    }

    /**
     * Sets a task as cancelled.
     * 
     * @param taskId Task Id.
     */
    protected static final void setCancelled(int taskId) {
        TASKS_TO_CANCEL.put(taskId, true);
    }
}