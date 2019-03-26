"""
This file contains all common functions for all PyCOMPSs for interactive in
Supercomputer related scripts.
"""

import os
import shlex
import subprocess

VERBOSE = False   # Boolean to print detailed information through stdout - For debugging purposes.
DECODING_FORMAT = 'utf-8'
SUCCESS_KEYWORD = 'SUCCESS'
NOT_RUNNING_KEYWORD = 'NOT_RUNING'
ERROR_KEYWORD = 'ERROR'
DISABLED_VALUE = 'undefined'
JOB_NAME_KEYWORD = '-PyCOMPSsInteractive'


def command_runner(cmd, exception=True):
    """
    Run the command defined in the cmd list.
    Decodes the stdout and stderr following the DECODING_FORMAT.
    :param cmd: Command to execute as list.
    :param exception: Throw exception if failed. Otherwise, the caller will handle the error.
    :return: return code, stdout, stderr
    """
    if VERBOSE:
        print("Executing command: " + ' '.join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()   # blocks until cmd is done
    return_code = p.returncode
    stdout = stdout.decode(DECODING_FORMAT)
    stderr = stderr.decode(DECODING_FORMAT)
    if exception and return_code != 0:
        _raise_command_exception(cmd, return_code, stdout, stderr)
    return return_code, stdout, stderr


def get_installation_path():
    """
    Retrieve the COMPSs installation root folder
    :return: COMPSs installation root folder
    """
    script_path = os.path.dirname(os.path.realpath(__file__))
    root_path = os.path.sep + os.path.join(*(script_path.split(os.path.sep)[:-4]))
    return root_path


def setup_supercomputer_configuration(include=None):
    """
    Setup the supercomputer configuration.
    Reads the supercomputer cfg and queuing system cfg and exports
    the variables defined in them within the current environment.
    This is useful to get the appropriate commands for the submission,
    cancel, status of jobs, etc. which depends on the specific supercomputer
    relying on the COMPSs configuration.
    Alternatively, it is possible to include a keyword in the command for
    the later substitution from the python code before using it (e.g.
    QUEUE_JOB_NAME_CMD - which is used multiple times).
    :param include: Dictionary with environment variables to consider in the
                    environment so that the cfgs can complete their contents
                    (e.g. job_id is needed for the command building variables).
    :return: None
    """
    if VERBOSE:
        print("Setting up the supercomputer configuration...")
    # Get the scripts directory
    sc_cfg_name = 'default.cfg'
    sc_cfg = os.path.join(get_installation_path(),
                          'Runtime', 'scripts', 'queues', 'supercomputers', sc_cfg_name)
    if VERBOSE:
        print("* Loading SC cfg file")
    _export_environment_variables(sc_cfg, include)
    # Export the environment variables
    qs_cfg_name = str(os.environ['QUEUE_SYSTEM'].strip()) + '.cfg'
    qs_cfg = os.path.join(get_installation_path(),
                          'Runtime', 'scripts', 'queues', 'queue_systems', qs_cfg_name)
    if VERBOSE:
        print("* Loading QS cfg file")
    _export_environment_variables(qs_cfg, include)


def is_notebook_job(job_id):
    """
    Checks if the given job id is running a PyCOMPSs notebook.
    To this end, checks the job name to see if it matches the JOB_NAME_KEYWORD.
    CAUTION: To use this function, the setup_supercomputer_configuration must have
             been performed.
    :param job_id: Job id to check
    :return: True if is a notebook. False on the contrary
    """
    name = get_job_name(job_id)
    if verify_job_name(name.strip()):
        if VERBOSE:
            print("Found notebook id: " + job_id)
        return True
    else:
        if VERBOSE:
            print("Job " + job_id + " is not a PyCOMPSs notebook job.")
        return False


def get_job_name(job_id):
    """
    Get the job name of a given job identifier.
    :param job_id: Job identifier
    :return: Job name
    """
    raw_job_name_command = os.environ['QUEUE_JOB_NAME_CMD']
    job_name_command = raw_job_name_command.replace('JOBID', str(job_id)).split()
    _, name, _ = command_runner(job_name_command)
    return name


def verify_job_name(name):
    """
    Verifies if the name provided includes the keyword
    :param name: Name to check
    :return: Boolean
    """
    if JOB_NAME_KEYWORD in name:
        return True
    else:
        return False


def not_a_notebook(job_id):
    """
    Prints the not a notebook job message and exit(1)
    :param job_id: Job id
    :return: None
    """
    print(ERROR_KEYWORD)
    print(" - Job Id: " + str(job_id) + " does not belong to a PyCOMPSs interactive job.")
    exit(1)


def _export_environment_variables(env_file, include=None):
    """
    Export the environment variables defined in "file".
    Uses bash to parse the file and gets the variables from a clean environment
    to read them and set them in the current environment.
    Includes the "include" variables in the environment if defined, so that
    the sourced configuration files can take them as inputs (e.g. job_id).
    :param env_file: File with the environment variables
    :param include: Dictionary with environment variables to consider in the
                    environment so that the cfgs can complete their contents
                    (e.g. job_id is needed for the command building variables).
    :return: None
    """
    if include:
        exports = []
        for k, v in include.items():
            exports.append('export ' + k.strip() + '=' + v.strip())
        exports = ' && '.join(exports)
        command = shlex.split("env -i bash -c 'set -a && " + exports + " && source " + str(env_file) + " && env'")
    else:
        command = shlex.split("env -i bash -c 'set -a && source " + str(env_file) + " && env'")
    if VERBOSE:
        print("Exporting environment variables from: " + env_file)
    return_code, stdout, stderr = command_runner(command)
    for line in stdout.splitlines():
        (key, _, value) = line.partition("=")
        if key != '_':
            os.environ[key] = value.strip()


def _raise_command_exception(command, return_code, stdout, stderr):
    """
    Generic exception raiser for command execution.
    :param command: Command that threw the exception
    :param return_code: Return code of the command that failed
    :param stdout: Standard output
    :param stderr: Standard error
    :return: None
    :raises: Exception
    """
    raise Exception(ERROR_KEYWORD + ": Failed execution: " + str(command)
                    + "\nRETURN CODE:" + str(return_code)
                    + "\nSTDOUT:\n" + str(stdout)
                    + "\nSTDERR:\n" + str(stderr))
