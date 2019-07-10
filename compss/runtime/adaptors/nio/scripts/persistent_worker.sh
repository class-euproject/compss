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

  # BLAUNCH start ----------------------------------------------------
  # Check that the current machine has not already awaken any WORKER in PORT and for app UUID
  worker_class="es.bsc.compss.nio.worker.NIOWorker"
  pid=$(ps -elfa | grep ${worker_class} | grep "${appUuid}" | grep "${hostName}" | grep "${worker_port}" | grep -v grep | awk '{ print $4 }')
  if [ "$pid" != "" ]; then
    if [ "$debug" == "true" ]; then
       echo "Worker already awaken. Nothing to do"
    fi
    echo "$pid"
    exit 0
  fi
 
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

  # shellcheck disable=SC2086
  setsid $cmd ${paramsToCOMPSsWorker} 1> "$workingDir/log/worker_${hostName}.out" 2> "$workingDir/log/worker_${hostName}.err" < /dev/null | echo "$!" &
  endCode=$?

  post_launch

  # Exit
  if [ $endCode -eq 0 ]; then
	exit 0
  else
	echo 1>&2 "[persistent_worker.sh] Worker could not be initalized"
	exit 7
  fi

