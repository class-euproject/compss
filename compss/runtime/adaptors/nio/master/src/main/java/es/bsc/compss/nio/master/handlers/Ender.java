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
package es.bsc.compss.nio.master.handlers;

import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.nio.master.WorkerStarter;


public class Ender extends Thread {

    private final WorkerStarter workerStarter;
    private final NIOWorkerNode node;
    private final int pid;


    /**
     * Creates a new Ender thread.
     * 
     * @param workerStarter Associated worker starter.
     * @param node Associated node.
     * @param pid Associated PID.
     */
    public Ender(WorkerStarter workerStarter, NIOWorkerNode node, int pid) {
        this.workerStarter = workerStarter;
        this.node = node;
        this.pid = pid;
    }

    @Override
    public void run() {
        this.workerStarter.ender(node, pid);
    }

}
