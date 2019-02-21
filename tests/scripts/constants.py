#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import os

############################################
# CONSTANTS
############################################

DEFAULT_SKIP = True
DEFAULT_NUM_RETRIES = 3
DEFAULT_FAIL_FAST = False
DEFAULT_FAMILIES = ["autoparallel",
                    "c",
                    "cloud",
                    "java",
                    "performance",
                    "pscos",
                    "python",
                    "tools"]
DEFAULT_TESTS = []

DEFAULT_CFG_FILE = "NIO_local.cfg"
DEFAULT_CFG_EXTENSION = ".cfg"
DEFAULT_COMPSS_HOME = "/opt/COMPSs/"
DEFAULT_COMM = "es.bsc.compss.nio.master.NIOAdaptor"
DEFAULT_EXECUTION_ENVS = ["python2", "python3"]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TESTS_DIR = os.path.join(SCRIPT_DIR, "../sources")
CONFIGURATIONS_DIR = os.path.join(SCRIPT_DIR, "../configurations")
RUNCOMPSS_REL_PATH = "Runtime/scripts/user/runcompss"
