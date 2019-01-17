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
package es.bsc.compss.types.fake;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;


public class FakeActionOrchestrator implements ActionOrchestrator {

    private final TaskScheduler ts;


    public FakeActionOrchestrator(TaskScheduler ts) {
        super();
        this.ts = ts;
    }

    // Notification thread
    @Override
    public void actionCompletion(AllocatableAction action) {
        ts.actionCompleted(action);
    }

    // Notification thread
    @Override
    public void actionError(AllocatableAction action) {
        ts.errorOnAction(action);
    }

}
