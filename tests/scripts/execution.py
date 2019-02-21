#!/usr/bin/python

# -*- coding: utf-8 -*-

# For better print formatting
from __future__ import print_function

# Imports
import os
from enum import Enum

from constants import RUNCOMPSS_REL_PATH


############################################
# ERROR CLASS
############################################

class TestExecutionError(Exception):
    """
    Class representing an error when executing the tests

    :attribute msg: Error message when executing the tests
        + type: String
    """

    def __init__(self, msg):
        """
        Initializes the TestExecutionError class with the given error message

        :param msg: Error message when executing the tests
        """
        self.msg = msg

    def __str__(self):
        return str(self.msg)


############################################
# HELPER CLASS
############################################

class ExitValue(Enum):
    OK = 0,
    OK_RETRY = 1,
    FAIL = 2


def _merge_exit_values(ev1, ev2):
    if ev1 == ExitValue.FAIL or ev2 == ExitValue.FAIL:
        return ExitValue.FAIL
    if ev1 == ExitValue.OK_RETRY or ev2 == ExitValue.OK_RETRY:
        return ExitValue.OK_RETRY
    return ExitValue.OK


############################################
# PUBLIC METHODS
############################################

def execute_tests(cmd_args, compss_cfg):
    """
    Executes all the deployed tests and builds a result summary table.
    If failfast option is set, once a test fails the script exits.

    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg:  Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return: An ExitValue object indicating the exit status of the WORST test execution
        + type: ExitValue
    :raise TestExecutionError: If an error is encountered when creating the necessary structures to launch the test
    """
    # Load deployment structure folder paths
    compss_logs_root = compss_cfg.get_compss_base_log_dir()
    target_base_dir = compss_cfg.get_target_base_dir()
    execution_sanbdox = os.path.join(target_base_dir, "apps")

    # Execute all the deployed tests
    results = []
    for test_dir in os.listdir(execution_sanbdox):
        test_path = os.path.join(execution_sanbdox, test_dir)
        ev = _execute_test(test_dir, test_path, compss_logs_root, cmd_args, compss_cfg)
        results.append((test_dir, ev))

        if cmd_args.fail_fast and ev == ExitValue.FAIL:
            print()
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print("[ERROR] Test has failed and fail-fast option is set. Aborting...")
            break

    # Process test results
    headers = ["Test G. Number", "Test Family", "Test F. Number", "Test Name", "Test Exec. Folder", "Test Result"]
    results_info = []
    global_ev = ExitValue.OK
    for test_dir, ev in results:
        global_ev = _merge_exit_values(global_ev, ev)
        test_global_num = int("".join(x for x in test_dir if x.isdigit()))
        test_name, _, family_dir, num_family = cmd_args.test_numbers["global"][test_global_num]
        results_info.append([test_global_num, family_dir, num_family, test_name, test_dir, ev.name])

    # Print result summary table
    from tabulate import tabulate
    print()
    print("----------------------------------------")
    print("TEST RESULTS SUMMARY:")
    print()
    print(tabulate(results_info, headers=headers))
    print("----------------------------------------")

    # Return if any test has failed
    return global_ev


############################################
# INTERNAL METHODS
############################################

def _execute_test(test_name, test_path, compss_logs_root, cmd_args, compss_cfg):
    """
    Executes the given test with the given options and retrieves its exit value

    :param test_name: Name of the test (on deployment phase: #appXXX)
        + type: String
    :param test_path: Path of the test (on deployment phase)
        + type: String
    :param compss_logs_root: Root folder of the COMPSs logs (usually ~/.COMPSs)
        + type: String
    :param cmd_args: Object representing the command line arguments
        + type: argparse.Namespace
    :param compss_cfg: Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return: An ExitValue object indicating the exit status of the test execution
        + type: ExitValue
    :raise TestExecutionError: If an error is encountered when creating the necessary structures to launch the test
    """
    print("[INFO] Executing test " + str(test_name))

    target_base_dir = compss_cfg.get_target_base_dir()
    logs_sanbdox = os.path.join(target_base_dir, "logs")

    max_retries = cmd_args.retry
    retry = 1
    test_ev = ExitValue.FAIL
    while test_ev == ExitValue.FAIL and retry <= max_retries:
        if __debug__:
            print("[DEBUG] Executing test " + str(test_name) + " Retry: " + str(retry) + "/" + str(max_retries))
        # Create logs folder for current retry
        test_logs_path = os.path.join(logs_sanbdox, test_name + "_" + str(retry))
        try:
            os.makedirs(test_logs_path)
        except OSError:
            raise TestExecutionError("[ERROR] Cannot create application log dir " + str(test_logs_path))
        # Execute test specific execution file
        test_ev = _execute_cmd(test_path, test_logs_path, compss_logs_root, retry, compss_cfg)
        retry = retry + 1

    # Return test exit value
    print("[INFO] Executed test " + str(test_name) + " with ExitValue " + test_ev.name)
    return test_ev


def _execute_cmd(test_path, test_logs_path, compss_logs_root, retry, compss_cfg):
    """
    Executes the execution script of a given test

    :param test_path: Path to the test deployment folder
        + type: String
    :param test_logs_path: Path to store the execution logs
        + type: String
    :param compss_logs_root: Path of the root COMPSs log folder
        + type: String
    :param retry: Retry number
        + type: int
    :param compss_cfg: Object representing the COMPSs test configuration options available in the given cfg file
        + type: COMPSsConfiguration
    :return: An ExitValue object indicating the exit status of the test execution
        + type: ExitValue
    :raise TestExecutionError: If an error is encountered when creating the necessary structures to launch the test
    """
    # Create command
    execution_script_path = os.path.join(test_path, "execution")
    if not os.path.isfile(execution_script_path):
        raise TestExecutionError("[ERROR] Cannot find execution script " + str(execution_script_path))

    runcompss_bin = compss_cfg.get_compss_home() + RUNCOMPSS_REL_PATH
    runcompss_user_opts = compss_cfg.get_runcompss_opts()
    if runcompss_user_opts is None:
        runcompss_user_opts = ""

    cmd = [str(execution_script_path),
           str(runcompss_bin),
           str(compss_cfg.get_comm()),
           str(runcompss_user_opts),
           str(test_path),
           str(compss_logs_root),
           str(test_logs_path),
           str(retry),
           str(compss_cfg.get_execution_envs_str())]

    if __debug__:
        print("[DEBUG] Test execution command: " + str(cmd))

    # Invoke execution script
    import subprocess
    try:
        exec_env = os.environ.copy()
        exec_env["JAVA_HOME"] = compss_cfg.get_java_home()
        exec_env["COMPSS_HOME"] = compss_cfg.get_compss_home()
        p = subprocess.Popen(cmd, cwd=test_path, env=exec_env, stdout=subprocess.PIPE)
        output, error = p.communicate()
        exit_value = p.returncode
    except Exception:
        exit_value = -1

    # Log command exit_value/output/error
    print("[INFO] Text execution command EXIT_VALUE: " + str(exit_value))
    print("[DEBUG] Text execution command OUTPUT: ")
    print(output)
    print("----------------------------------------")
    print("[ERROR] Text execution command ERROR: ")
    print(error)
    print("----------------------------------------")

    if p.returncode == 0:
        if retry == 1:
            return ExitValue.OK
        return ExitValue.OK_RETRY
    return ExitValue.FAIL
