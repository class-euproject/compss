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
package es.bsc.compss.invokers.util;

import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.log.Loggers;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The JobQueue class is an utility to enqueue job execution requests.
 */
public class JobQueue {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    /** Job requests. **/
    private final BlockingQueue<Execution> queue;

    /** Stack storing the objects blocking the waiting threads. */
    private BlockingDeque<Object> waitingLocks;


    /**
     * Constructs a new RequestQueue without any pending request nor asleep threads waiting for requests.
     */
    public JobQueue() {
        this.queue = new LinkedBlockingQueue<>();
        this.waitingLocks = new LinkedBlockingDeque<>();
    }

    /**
     * Adds a request at the tail of the queue.
     * 
     * @param request Request to be added
     */
    public void enqueue(Execution request) {
        // Add new job to queue
        if (request.getInvocation() == null) {
            LOGGER.debug("Enqueueing null");
        } else {
            LOGGER.debug("Enqueueing job " + request.getInvocation().getJobId());
        }
        synchronized (this) {
            this.queue.add(request);

            // Wake up the last executor thread if any
            if (!this.waitingLocks.isEmpty()) {
                Object lock = this.waitingLocks.pop();
                synchronized (lock) {
                    LOGGER.debug("Releasing lock " + lock.hashCode());
                    lock.notify();
                }
            }
        }
    }

    /**
     * Dequeues a request from the queue. If there are no pending requests, the current Thread falls asleep until a new
     * Request is added. This request will be return value of the method.
     * 
     * @return the first request from the queue
     */
    public Execution dequeue() {
        Execution exec = null;
        Object lock = new Object();
        while (exec == null) {
            synchronized (lock) {
                synchronized (this) {
                    exec = this.queue.poll();
                    if (exec != null) {
                        continue;
                    }
                    // The queue is empty, register the thread on the waiting stack
                    this.waitingLocks.push(lock);
                }
                try {
                    lock.wait();
                } catch (InterruptedException ie) {
                    LOGGER.error("ERROR: Job Thread was interrupted while waiting for next job", ie);
                    return null;
                }
            }
        }
        /*
         * while ((exec = this.queue.poll()) == null) { // The queue is empty, register the thread on the waiting stack
         * Object lock = new Object(); this.waitingLocks.push(lock); try { synchronized (lock) { lock.wait(); } } catch
         * (InterruptedException ie) { LOGGER.error("ERROR: Job Thread was interrupted while waiting for next job", ie);
         * return null; } }
         */

        // The queue is not empty, take the first available job
        return exec;
    }

    /**
     * Sends a signal to all the Threads that are waiting for a request.
     */
    public void wakeUpAll() {
        // Wake up all waiting threads
        LOGGER.info("Waking up " + waitingLocks.size() + " locks.");
        while (!this.waitingLocks.isEmpty()) {
            Object lock = this.waitingLocks.pop();
            synchronized (lock) {
                LOGGER.debug("Release lock" + lock.hashCode());
                lock.notify();
            }
        }
    }

}
