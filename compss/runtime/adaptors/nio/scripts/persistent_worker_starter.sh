#!/bin/bash

  # shellcheck disable=SC2154
  # Because many variables are sourced from common setup.sh


  ######################
  # MAIN PROGRAM
  ######################

  # Load common setup functions --------------------------------------
  if [ -z "${COMPSS_HOME}" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export COMPSS_HOME=${SCRIPT_DIR}/../../../../../
  else
    SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/system/adaptors/nio"
  fi
  # shellcheck source=setup.sh
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}"/setup.sh

  # Load parameters --------------------------------------------------
  load_parameters "$@"

  # Trap to clean environment
  trap clean_env EXIT

  # Normal start -----------------------------------------------------
  # Setup
  setup_environment
  setup_extrae
  setup_jvm

  # Launch the Worker JVM
  pre_launch  

  reprogram_fpga

  if [ "$debug" == "true" ]; then
      export COMPSS_BINDINGS_DEBUG=1
      export NX_ARGS="--summary"
      export NANOS6=debug

      echo "[persistent_worker.sh] Calling NIOWorker of host ${hostName}"
      echo "Calling NIOWorker"
      echo "Cmd: $cmd ${paramsToCOMPSsWorker}"
  fi

  if [ -n "$cusGPU" ] && [ "$cusGPU" -gt 0 ]; then
    echo "Computing units GPU is greater than zero, Nanos6 scheduler set to hierarchical"
    export NANOS6_SCHEDULER=hierarchical
  fi

  $cmd ${paramsToCOMPSsWorker} 2> $workingDir/log/worker_${hostName}.err | tee $workingDir/log/worker_${hostName}.out

  exitValue=$?

  post_launch

  # Exit with the worker status (last command)
  if [ "$debug" == "true" ]; then
    echo "[persistent_worker.sh] Exit NIOWorker of host ${hostName} with exit value ${exitValue}"
  fi
  exit $exitValue
