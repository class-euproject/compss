#!/bin/bash

  NUM_PARAMS=32  

  ######################
  # INTERNAL FUNCTIONS
  ######################
  add_to_classpath () {
    local DIRLIBS=${1}/*.jar
    for i in ${DIRLIBS}; do
      if [ "$i" != "${DIRLIBS}" ] ; then
        CLASSPATH=$CLASSPATH:"$i"
      fi
    done
    export CLASSPATH=$CLASSPATH
  }
  # Displays runtime/application errors
  error_msg() {
    local error_msg=$1

    # Display error
    echo
    echo "$error_msg"
    echo

    # Exit
    exit 1
  }

  ######################
  # COMMON HELPER FUNCTIONS
  ######################
  load_parameters() {
    # Script Variables
    if [ -z "${SCRIPT_DIR}" ]; then
        if [ -z "$COMPSS_HOME" ]; then
           SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
        else
           SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/system/adaptors/nio"
        fi
    fi
    # Get parameters
    libPath=$1
    appDir=$2
    cp=$3
    numJvmFlags=$4

    jvmFlags=""
    for i in $(seq 1 "$numJvmFlags"); do
      pos=$((4 + i))
      jvmFlags="${jvmFlags} ${!pos}"
    done

    # Shift parameters for script and leave only the NIOWorker parameters
    paramsToShift=$((4 + numJvmFlags))
    shift ${paramsToShift}

    FPGAargs=""
    numFPGAargs=$1
    if [ "$numFPGAargs" -gt 0 ]; then
      for i in $(seq 1 "$numFPGAargs"); do
        pos=$((1 + i))
        FPGAargs="${FPGAargs} ${!pos}"
      done
    fi

    # Shift parameters for script and leave only the NIOWorker parameters
    paramsToShift=$((1 + numFPGAargs))
    shift ${paramsToShift}

    paramsToCOMPSsWorker=$@

    # Check number of parameters
    if [ $# -ne ${NUM_PARAMS} ]; then
        echo "ERROR: Incorrect number of parameters. Expected: ${NUM_PARAMS}. Got: $#"
        exit 1
    fi

    # Catch some NIOWorker parameters
    debug=$1
    hostName=$4
    worker_port=$5
    cusCPU=$8
    cusGPU=$9
    cusFPGA=${10}
    lot=${14}
    appUuid=${15}
    lang=${16}
    workingDir=${17}
    installDir=${18}
    appDirNW=${19}
    libPathNW=${20}
    cpNW=${21}
    pythonpath=${22}
    tracing=${23}
    extraeFile=${24}
    hostId=${25}
    storageConf=${26}
    execType=${27}
    persistentBinding=${28}
    pythonInterpreter=${29}
    pythonVersion=${30}
    pythonVirtualEnvironment=${31}
    pythonPropagateVirtualEnvironment=${32}

    if [ "$debug" == "true" ]; then
      echo "PERSISTENT_WORKER.sh"
      echo "- HostName:            $hostName"
      echo "- WorkerPort:          ${worker_port}"
      echo "- WorkingDir:          $workingDir"
      echo "- InstallDir:          $installDir"

      echo "- Computing Units CPU: ${cusCPU}"
      echo "- Computing Units GPU: ${cusGPU}"
      echo "- Computing Units GPU: ${cusFPGA}"
      echo "- Limit Of Tasks:      ${lot}"
      echo "- JVM Opts:            $jvmFlags"

      echo "- AppUUID:             ${appUuid}"
      echo "- Lang:                ${lang}"
      echo "- AppDir:              $appDirNW"
      echo "- libPath:             $libPathNW"
      echo "- Classpath:           $cpNW"
      echo "- Pythonpath:          $pythonpath"
      echo "- Python Interpreter   $pythonInterpreter"
      echo "- Python Version       $pythonVersion"
      echo "- Python Virtual Env.  $pythonVirtualEnvironment"
      echo "- Python Propagate Virtual Env.  $pythonPropagateVirtualEnvironment"

      echo "- Tracing:             $tracing"
      echo "- ExtraeFile:          ${extraeFile}"
      echo "- HostId:              ${hostId}"
      echo "- StorageConf:         ${storageConf}"
      echo "- ExecType:            ${execType}"
      echo "- Persistent:          ${persistentBinding}"
    fi

    # Calculate Log4j file
    if [ "${debug}" == "true" ]; then
      itlog4j_file=COMPSsWorker-log4j.debug
    else
      itlog4j_file=COMPSsWorker-log4j.off
    fi

    # Calculate must erase working dir
    if [[ "$jvmFlags" == *"-Dcompss.worker.removeWD=false"* ]]; then
      eraseWD="false"
    else
      eraseWD="true"
    fi
  }

  setup_extrae() {
    # Trace initialization
    if [ "$tracing" -gt 0 ]; then
      if [ -z "${extraeFile}" ] || [ "${extraeFile}" == "null" ]; then
        # Only define extraeFile if it is not a custom location
        extraeFile=${SCRIPT_DIR}/../../../../configuration/xml/tracing/extrae_basic.xml
        if [ "$tracing" -gt 1 ]; then
          extraeFile=${SCRIPT_DIR}/../../../../configuration/xml/tracing/extrae_advanced.xml
        fi
      fi  

      if [ -z "$EXTRAE_HOME" ]; then
        export EXTRAE_HOME=${SCRIPT_DIR}/../../../../../Dependencies/extrae/
      fi  

      export EXTRAE_LIB=${EXTRAE_HOME}/lib
      export LD_LIBRARY_PATH=${EXTRAE_LIB}:${LD_LIBRARY_PATH}
      export EXTRAE_CONFIG_FILE=${extraeFile}
      export LD_PRELOAD=${EXTRAE_HOME}/lib/libpttrace.so
    fi  
  }

  setup_environment(){
    # Added for SGE queue systems which do not allow to copy LD_LIBRARY_PATH
    if [ -z "$LD_LIBRARY_PATH" ]; then
        if [ -n "$LIBRARY_PATH" ]; then
            export LD_LIBRARY_PATH=$LIBRARY_PATH
            echo "[  INFO] LD_LIBRARY_PATH not defined set to LIBRARY_PATH"
        fi
    fi

    # Create sandbox
    if [ ! -d "$workingDir" ]; then
  	mkdir -p "$workingDir"
    fi
    export COMPSS_WORKING_DIR=$workingDir
    mkdir -p "$workingDir"/log
    mkdir -p "$workingDir"/jobs

    # Look for the JVM Library
    libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.so | head -n 1)
    if [ -z "$libjava" ]; then
        libjava=$(find "${JAVA_HOME}"/jre/lib/ -name libjvm.dylib | head -n 1)
    fi
    if [ -n "$libjava" ]; then
        libjavafolder=$(dirname "$libjava")
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$libjavafolder
    fi

    # Set lib path
    if [ "$libPath" != "null" ]; then
        export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$libPath
    fi

    # Set appDir
    export COMPSS_APP_DIR=$appDir
    if [ "$appDir" != "null" ]; then
    	add_to_classpath "$appDir"
    	add_to_classpath "$appDir/lib"
    fi

    # Set the classpath
    if [ "$cp" == "null" ]; then
  	cp=""
    fi

    # Export environment
    export CLASSPATH=$cpNW:$CLASSPATH
    export PYTHONPATH=$pythonpath:$PYTHONPATH
    export LD_LIBRARY_PATH=$libPathNW:${SCRIPT_DIR}/../../../../../Bindings/bindings-common/lib:${SCRIPT_DIR}/../../../../../Bindings/c/lib:$LD_LIBRARY_PATH
  }

  setup_jvm() {
    # Prepare the worker command
    local JAVA=java
    worker_jar=${SCRIPT_DIR}/../../../../adaptors/nio/worker/compss-adaptors-nio-worker.jar
    local main_worker_class=es.bsc.compss.nio.worker.NIOWorker
    worker_jvm_flags="${jvmFlags} \
    -XX:+PerfDisableSharedMem \
    -XX:-UsePerfData \
    -XX:+UseG1GC \
    -XX:+UseThreadPriorities \
    -XX:ThreadPriorityPolicy=42 \
    -Dlog4j.configurationFile=${installDir}/Runtime/configuration/log/${itlog4j_file} \
    -Dcompss.python.interpreter=${pythonInterpreter} \
    -Dcompss.python.version=${pythonVersion} \
    -Dcompss.python.virtualenvironment=${pythonVirtualEnvironment} \
    -Dcompss.python.propagate_virtualenvironment=${pythonPropagateVirtualEnvironment} \
    -Djava.library.path=$LD_LIBRARY_PATH"
    
    if [ "$lang" = "c" ] && [ "${persistentBinding}" = "true" ]; then
    	generate_jvm_opts_file
    	cmd="${appDir}/worker/nio_worker_c"
    else
        cmd="$JAVA ${worker_jvm_flags} -classpath $CLASSPATH:${worker_jar} ${main_worker_class}"
    fi
    	
  }
  
  generate_jvm_opts_file() {
    jvm_worker_opts=$(echo $worker_jvm_flags | tr " " "\n")
    jvm_options_file=$(mktemp) || error_msg "Error creating java_opts_tmp_file"
    cat >> "${jvm_options_file}" << EOT
${jvm_worker_opts}
-Djava.class.path=$CLASSPATH:${worker_jar}
EOT
  }

  reprogram_fpga() {
    if [ -n "${FPGAargs}" ]; then
        echo "Reprogramming FPGA with the command "${FPGAargs}""
        eval "$FPGAargs"
    fi
  }

  pre_launch() {
    cd "$workingDir" || exit 1
    
    if [ "${persistentBinding}" = "true" ]; then
    	export COMPSS_HOME=${SCRIPT_DIR}/../../../../../
    	export LD_LIBRARY_PATH=${COMPSS_HOME}/Bindings/bindings-common/lib:${COMPSS_HOME}/Bindings/c/lib:${LD_LIBRARY_PATH}
		export JVM_OPTIONS_FILE=${jvm_options_file}
    fi
  }

  post_launch() {
    if [ "$tracing" -gt 0 ]; then
      unset LD_PRELOAD
    fi
  }

  clean_env() {
    if [ "$eraseWD" = "true" ]; then
      echo "[persistent_worker.sh] Clean WD ${workingDir}"
      rm -rf "${workingDir}"
    else
      echo "[persistent_worker.sh] Not cleaning WD ${workingDir}"
    fi
  }
