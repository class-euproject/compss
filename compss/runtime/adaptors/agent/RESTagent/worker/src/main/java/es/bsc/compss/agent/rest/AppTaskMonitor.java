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
package es.bsc.compss.agent.rest;

import es.bsc.compss.agent.Agent.AppMonitor;
import es.bsc.compss.types.annotations.parameter.DataType;


public class AppTaskMonitor extends AppMonitor {

    private final String[] paramResults;
    private boolean successful;

    public AppTaskMonitor(int numParams) {
        super();
        this.successful = false;
        this.paramResults = new String[numParams];
    }

    @Override
    public void onCreation() {
    }

    @Override
    public void onAccessesProcessed() {
    }

    @Override
    public void onSchedule() {
    }

    @Override
    public void onSubmission() {
    }

    @Override
    public void valueGenerated(int paramId, DataType type, Object value) {
        paramResults[paramId] = value.toString();
    }

    @Override
    public void onErrorExecution() {
    }

    @Override
    public void onFailedExecution() {
        successful = false;
    }

    @Override
    public void onSuccesfulExecution() {
        successful = true;
    }

    /*@Override
    public void completed() {
        try {
            if (orchestrator != null) {
                String masterId = orchestrator.getHost();
                String operation = orchestrator.getOperation();
                WebTarget target = client.target(masterId);
                WebTarget wt = target.path(operation);
                EndApplicationNotification ean = new EndApplicationNotification(
                        "" + getAppId(),
                        successful ? JobEndStatus.OK : JobEndStatus.EXECUTION_FAILED,
                        paramResults);
                Debugger.debug("AGENT", "Submitting Job End");
                Debugger.debugAsXML(ean);

                Response response = wt
                        .request(MediaType.APPLICATION_JSON)
                        .put(Entity.xml(ean), Response.class);
                if (response.getStatusInfo().getStatusCode() != 200) {
                    Debugger.err("AGENT", "Could not notify Application " + getAppId() + " end to " + wt);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
    @Override
    public void onCompletion() {

    }

    @Override
    public void onFailure() {

    }
}
