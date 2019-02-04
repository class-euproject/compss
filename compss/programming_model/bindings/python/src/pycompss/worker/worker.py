#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 

# -*- coding: utf-8 -*-

"""
PyCOMPSs Worker
===============
    This file contains the worker code.
    Args: debug full_path (method_class)
    method_name has_target num_params par_type_1 par_1 ... par_type_n par_n
"""

import logging
import os
import sys

from pycompss.runtime.commons import IS_PYTHON3
from pycompss.runtime.commons import TRACING
from pycompss.util.logs import init_logging_worker
from pycompss.worker.worker_commons import execute_task

if IS_PYTHON3:
    long = int
else:
    # Exception moved to built-in
    str_escape = 'string_escape'

SYNC_EVENTS = 8000666

# Should be equal to Tracer.java definitions
TASK_EVENTS = 60000100

# Rank 110-119 reserved to events launched from task.py
PROCESS_CREATION = 100
WORKER_INITIALIZATION = 102
PARAMETER_PROCESSING = 103
LOGGING = 104
MODULES_IMPORT = 105
WORKER_END = 106
PROCESS_DESTRUCTION = 107


# Uncomment the next line if you do not want to reuse pyc files.
# sys.dont_write_bytecode = True


def compss_worker(tracing, task_id, storage_conf, args):
    """
    Worker main method (invocated from __main__).

    :param tracing: Tracing boolean
    :param task_id: Task identifier
    :param storage_conf: Storage configuration file
    :param args: Arguments following the common order
    :return: Exit code
    """

    if __debug__:
        logger = logging.getLogger('pycompss.worker.worker')
        logger.debug("Starting Worker")

    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    exit_code, _, _ = execute_task("Task " + task_id, storage_conf, args, tracing, logger)

    if __debug__:
        logger.debug("Finishing Worker")

    return exit_code


def main():
    # Emit sync event if tracing is enabled
    TRACING = sys.argv[1] == 'true'
    task_id = int(sys.argv[2])
    log_level = sys.argv[3]
    storage_conf = sys.argv[4]
    method_type = sys.argv[5]
    args = sys.argv[6:]
    # class_name = sys.argv[6]
    # method_name = sys.argv[7]
    # num_slaves = sys.argv[8]
    # i = 8 + num_slaves
    # slaves = sys.argv[9-i]
    # numCus = sys.argv[i+1]
    # has_target = sys.argv[i+2] == 'true'
    # num_params = int(sys.argv[i+3])
    # params = sys.argv[i+4..]

    print("tracing = " + str(TRACING))
    print("task_id = " + str(task_id))
    print("log_level = " + str(log_level))
    print("storage_conf = " + str(storage_conf))

    persistent_storage = False
    if storage_conf != 'null':
        persistent_storage = True
        from storage.api import initWorker as initStorageAtWorker
        from storage.api import finishWorker as finishStorageAtWorker

    if TRACING:
        import pyextrae.multiprocessing as pyextrae

        pyextrae.eventandcounters(SYNC_EVENTS, task_id)
        # pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(TASK_EVENTS, WORKER_INITIALIZATION)

    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    if log_level == 'true' or log_level == "debug":
        # Debug
        init_logging_worker(worker_path + '/../../log/logging_debug.json')
    elif log_level == "info" or log_level == "off":
        # Info or no debug
        init_logging_worker(worker_path + '/../../log/logging_off.json')
    else:
        # Default
        init_logging_worker(worker_path + '/../../log/logging.json')

    if persistent_storage:
        # Initialize storage
        initStorageAtWorker(config_file_path=storage_conf)

    # Init worker
    exit_code = compss_worker(TRACING, str(task_id), storage_conf, args)

    if TRACING:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        # pyextrae.eventandcounters(TASK_EVENTS, PROCESS_DESTRUCTION)
        pyextrae.eventandcounters(SYNC_EVENTS, task_id)

    if persistent_storage:
        # Finish storage
        finishStorageAtWorker()

    if exit_code == 1:
        exit(1)


if __name__ == '__main__':
    main()
