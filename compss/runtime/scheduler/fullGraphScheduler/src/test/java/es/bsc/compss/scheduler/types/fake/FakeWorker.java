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
package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceType;
import es.bsc.compss.types.resources.Worker;


public class FakeWorker extends Worker<FakeResourceDescription> {

    private final FakeResourceDescription available;


    public FakeWorker(String name, FakeResourceDescription description, int limitOfTasks) {
        super(name, description, new FakeNode(name), limitOfTasks, null);
        this.available = (FakeResourceDescription) description.copy();
    }

    public FakeWorker(FakeWorker fw) {
        super(fw);
        this.available = (FakeResourceDescription) fw.available.copy();
    }

    @Override
    public ResourceType getType() {
        return ResourceType.WORKER;
    }

    @Override
    public int compareTo(Resource rsrc) {
        return 0;
    }

    @Override
    public String getMonitoringData(String prefix) {
        return "";
    }

    @Override
    public FakeResourceDescription reserveResource(FakeResourceDescription consumption) {
        synchronized (this.available) {
            if (this.hasAvailable(consumption)) {
                return (FakeResourceDescription) this.available.reduceDynamic(consumption);
            } else {
                return null;
            }
        }
    }

    @Override
    public void releaseResource(FakeResourceDescription consumption) {
        synchronized (this.available) {
            this.available.increaseDynamic(consumption);
        }
    }

    @Override
    public void releaseAllResources() {
        synchronized (this.available) {
            super.resetUsedTaskCounts();
            this.available.reduceDynamic(this.available);
            this.available.increaseDynamic(this.description);
        }
    }

    @Override
    public boolean hasAvailable(FakeResourceDescription consumption) {
        synchronized (this.available) {
            return this.available.canHost(consumption);
        }
    }

    @Override
    public boolean canRun(Implementation implementation) {
        return true;
    }

    @Override
    public Integer fitCount(Implementation impl) {
        return 10;
    }

    @Override
    public boolean hasAvailableSlots() {
        return true;
    }

    @Override
    public Worker<FakeResourceDescription> getSchedulingCopy() {
        return new FakeWorker(this);
    }
}
