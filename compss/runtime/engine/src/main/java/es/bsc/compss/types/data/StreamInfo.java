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
package es.bsc.compss.types.data;

import java.util.concurrent.Semaphore;


public class StreamInfo extends DataInfo {

    // Hash code of the object
    private int code;


    /**
     * Creates a new StreamInfo instance with the given hashcode.
     * 
     * @param code Object hashcode.
     */
    public StreamInfo(int code) {
        super();
        this.code = code;
    }

    /**
     * Returns the object hashcode.
     * 
     * @return The object hashcode.
     */
    public int getCode() {
        return this.code;
    }

    @Override
    public void willBeWritten() {
        // Do not increase version on write, since Stream just publish values
        this.currentVersion.willBeWritten();
        this.currentVersion.versionUsed();
    }

    @Override
    public int waitForDataReadyToDelete(Semaphore semWait) {
        // Nothing to wait for
        return 0;
    }

}
