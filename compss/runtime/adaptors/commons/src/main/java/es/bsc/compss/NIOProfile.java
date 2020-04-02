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
package es.bsc.compss;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class NIOProfile implements Externalizable {

    private long timeStart;

    private long timeEnd;

    private long timeStartMaster;

    private long timeEndMaster;


    /**
     * Only for externalization.
     */
    public NIOProfile() {
        // All attributes are initialized statically
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.timeStart = in.readLong();
        this.timeEnd = in.readLong();
        this.timeStartMaster = this.timeEndMaster = 0;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(this.timeStart);
        out.writeLong(this.timeEnd);
    }

    public void start() {
        this.timeStart = System.currentTimeMillis();
    }

    public void end() {
        this.timeEnd = System.currentTimeMillis();
    }

    public long getStartTime() {
        return timeStart;
    }

    public long getEndTime() {
        return timeEnd;
    }

    public void setStartTimeMaster(long time) {
        this.timeStartMaster = time;
    }

    public void setEndTimeMaster(long time) {
        this.timeEndMaster = time;
    }

    public long getStartTimeMaster() {
        return timeStartMaster;
    }

    public long getEndTimeMaster() {
        return timeEndMaster;
    }

}