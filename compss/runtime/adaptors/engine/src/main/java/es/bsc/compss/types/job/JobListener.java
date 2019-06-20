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
package es.bsc.compss.types.job;

import es.bsc.compss.types.job.JobEndStatus;

/**
 * Abstract Representation of a listener for the job execution.
 */
public interface JobListener {

    /**
     * Actions when job has successfully ended.
     * 
     * @param job Job to notify completion.
     */
    public void jobCompleted(Job<?> job);

    /**
     * Actions when job has failed.
     * 
     * @param job Job to notify completion.
     * @param endStatus Failure status.
     */
    public void jobFailed(Job<?> job, JobEndStatus endStatus);

}
