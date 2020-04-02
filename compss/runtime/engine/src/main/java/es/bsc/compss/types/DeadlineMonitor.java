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
package es.bsc.compss.types;

import es.bsc.compss.NIOProfile;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.types.annotations.parameter.DataType;


public class DeadlineMonitor implements TaskMonitor {

    private NIOProfile p;


    public DeadlineMonitor() {
        this.p = null;
    }

    @Override
    public void onCreation() {
        // NOTHING TO DO
    }

    @Override
    public void onAccessesProcessed() {
        // NOTHING TO DO
    }

    @Override
    public void onSchedule() {
        // NOTHING TO DO
    }

    @Override
    public void onSubmission() {
        // NOTHING TO DO
    }

    @Override
    public void valueGenerated(int paramId, String paramName, DataType paramType, String dataId, Object dataLocation) {
        // NOTHING TO DO
    }

    @Override
    public void onAbortedExecution() {
        // NOTHING TO DO
    }

    @Override
    public void onErrorExecution() {
        // NOTHING TO DO
    }

    @Override
    public void onFailedExecution() {
        // NOTHING TO DO
    }

    @Override
    public void onSuccesfulExecution() {
        // NOTHING TO DO
    }

    public void onSuccesfulExecution(NIOProfile p) {
        this.p = p;
    }

    @Override
    public void onCancellation() {
        // NOTHING TO DO
    }

    @Override
    public void onCompletion() {
        // NOTHING TO DO
    }

    @Override
    public void onFailure() {
        // NOTHING TO DO
    }

    @Override
    public void onException() {
        // NOTHING TO DO
    }

    public NIOProfile getProfile() {
        return this.p;
    }
}
