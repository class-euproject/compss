"""
This script is aimed at cancelling a running PyCOMPSs job with Jupyter notebook.
"""

import os
import sys
from commons import VERBOSE
from commons import SUCCESS_KEYWORD
from commons import ERROR_KEYWORD
from commons import setup_supercomputer_configuration
from commons import update_command
from commons import command_runner
from commons import is_notebook_job
from commons import not_a_notebook


def cancel():
    """
    Main cancel function.
    :return: None
    """
    if VERBOSE:
        print("Cancelling PyCOMPSs interactive job...")

    # Get command line arguments
    job_ids = sys.argv[1:]

    # Load the Supercomputer configuration to get the appropriate cancel command
    setup_supercomputer_configuration()

    success = True
    # There might be more than one to cancel
    for job_id in job_ids:
        if VERBOSE:
            print(" - Job: " + str(job_id))

        # Check if the job_id belongs to a notebook before continuing
        if not is_notebook_job(job_id):
            not_a_notebook(job_id)

        # Get the command to cancel the job
        raw_cancel_command = os.environ['QUEUE_JOB_CANCEL_CMD']
        cancel_command = update_command(raw_cancel_command, job_id)

        # Cancel the job
        return_code, _, _ = command_runner(cancel_command)

        if return_code != 0:
            success = False

    if VERBOSE:
        print("Finished cancellation.")

    # Print to notify the cancel result
    if success:
        print(SUCCESS_KEYWORD)
    else:
        print(ERROR_KEYWORD)


if __name__ == '__main__':
    cancel()
