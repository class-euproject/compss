#!/bin/bash

#---------------------------------------------------
# SCRIPT CONSTANTS DECLARATION
#---------------------------------------------------
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

DEFAULT_SC_CFG="default"
DEFAULT_JOB_NAME="COMPSs"
DEFAULT_CPUS_PER_TASK="false"

#---------------------------------------------------
# ERROR CONSTANTS DECLARATION
#---------------------------------------------------
ERROR_NUM_NODES="Invalid number of nodes"
ERROR_NUM_CPUS="Invalid number of CPUS per node"
ERROR_SWITCHES="Too little switches for the specified number of nodes"
ERROR_NO_ASK_SWITCHES="Cannot ask switches for less than ${MIN_NODES_REQ_SWITCH} nodes"
ERROR_NODE_MEMORY="Incorrect node_memory parameter. Only disabled or <int> allowed. I.e. 33000, 66000"
ERROR_TMP_FILE="Cannot create TMP Submit file"
ERROR_STORAGE_PROPS="storage_props flag not defined"
ERROR_STORAGE_PROPS_FILE="storage_props file doesn't exist"
ERROR_PROJECT_NAME_NA="Project name not defined (use --project_name flag)"


#---------------------------------------------------------------------------------------
# HELPER FUNCTIONS
#---------------------------------------------------------------------------------------

###############################################
# Displays usage
###############################################
usage() {
  local exitValue=$1

  cat <<EOT
Usage: $0 [options] application_name application_arguments

* Options:
  General:
    --help, -h                              Print this help message

    --opts                                  Show available options

    --version, -v                           Print COMPSs version

    --sc_cfg=<name>                         SuperComputer configuration file to use. Must exist inside queues/cfgs/
                                            Mandatory
                                            Default: ${DEFAULT_SC_CFG}

  Submission configuration:
EOT

  show_opts "$exitValue"
}

###############################################
# Show Options
###############################################
show_opts() {
  local exitValue=$1

  # Load default CFG for default values
  local defaultSC_cfg="${SCRIPT_DIR}/../supercomputers/${DEFAULT_SC_CFG}.cfg"
  # shellcheck source=../supercomputers/default.cfg
  # shellcheck disable=SC1091
  source "${defaultSC_cfg}"
  local defaultQS_cfg="${SCRIPT_DIR}/../queue_systems/${QUEUE_SYSTEM}.cfg"
  # shellcheck source=../queue_systems/slurm.cfg
  # shellcheck disable=SC1091
  source "${defaultQS_cfg}"

  # Show usage
  cat <<EOT
  General submision arguments:
    --exec_time=<minutes>                   Expected execution time of the application (in minutes)
                                            Default: ${DEFAULT_EXEC_TIME}
    --job_name=<name>                       Job name
                                            Default: ${DEFAULT_JOB_NAME}
    --queue=<name>                          Queue name to submit the job. Depends on the queue system.
                                            For example (MN3): bsc_cs | bsc_debug | debug | interactive
                                            Default: ${DEFAULT_QUEUE}
    --reservation=<name>                    Reservation to use when submitting the job.
                                            Default: ${DEFAULT_RESERVATION}
EOT
   if [ -z "${DISABLE_QARG_CONSTRAINTS}" ] || [ "${DISABLE_QARG_CONSTRAINTS}" == "false" ]; then
    cat <<EOT
    --constraints=<constraints>		    Constraints to pass to queue system.
					    Default: ${DEFAULT_CONSTRAINTS}
EOT
   fi
   if [ "${ENABLE_PROJECT_NAME}" == "true" ]; then
    cat <<EOT

    --project_name=<name>                   Project name to pass to queue system.
                                            Mandatory for systems that charge hours by project name.
EOT
  fi
  if [ -z "${DISABLE_QARG_QOS}" ] || [ "${DISABLE_QARG_QOS}" == "false" ]; then
    cat <<EOT
    --qos=<qos>                             Quality of Service to pass to the queue system.
                                            Default: ${DEFAULT_QOS}
EOT
  fi

  if [ -z "${DISABLE_QARG_CPUS_PER_TASK}" ] || [ "${DISABLE_QARG_CPUS_PER_TASK}" == "false" ]; then
    cat <<EOT
    --cpus_per_task                         Number of cpus per task the queue system must allocate per task.
                                            Note that this will be equal to the cpus_per_node in a worker node and
                                            equal to the worker_in_master_cpus in a master node respectively.
                                            Default: ${DEFAULT_CPUS_PER_TASK}
EOT
  fi
    cat <<EOT
    --job_dependency=<jobID>                Postpone job execution until the job dependency has ended.
                                            Default: ${DEFAULT_DEPENDENCY_JOB}
    --storage_home=<string>                 Root installation dir of the storage implementation
                                            Default: ${DEFAULT_STORAGE_HOME}
    --storage_props=<string>                Absolute path of the storage properties file
                                            Mandatory if storage_home is defined
  Normal submission arguments:
    --num_nodes=<int>                       Number of nodes to use
                                            Default: ${DEFAULT_NUM_NODES}
    --num_switches=<int>                    Maximum number of different switches. Select 0 for no restrictions.
                                            Maximum nodes per switch: ${MAX_NODES_SWITCH}
                                            Only available for at least ${MIN_NODES_REQ_SWITCH} nodes.
                                            Default: ${DEFAULT_NUM_SWITCHES}
  Heterogeneous submission arguments:
    --type_cfg=<file_location>              Location of the file with the descriptions of node type requests
                                            File should follow the following format:
                                            type_X(){
                                              cpus_per_node=24
                                              node_memory=96
                                              ...
                                            }
                                            type_Y(){
                                              ...
                                            }
    --master=<master_node_type>             Node type for the master
                                            (Node type descriptions are provided in the --type_cfg flag)
    --workers=type_X:nodes,type_Y:nodes     Node type and number of nodes per type for the workers
                                            (Node type descriptions are provided in the --type_cfg flag)
  Launch configuration:
EOT
  "${SCRIPT_DIR}"/../../user/launch_compss --opts

  exit "$exitValue"
}

###############################################
# Displays version
###############################################
display_version() {
  local exitValue=$1

  "${SCRIPT_DIR}"/../../user/runcompss --version

  exit "$exitValue"
}

###############################################
# Displays errors when treating arguments
###############################################
display_error() {
  local error_msg=$1

  echo "$error_msg"
  echo " "

  usage 1
}

###############################################
# Displays errors when executing
###############################################
display_execution_error() {
  local error_msg=$1

  echo "$error_msg"
  echo " "

  exit 1
}

###############################################
# Function to log the arguments
###############################################
log_args() {
  # Display generic arguments
  echo "SC Configuration:          ${sc_cfg}"
  echo "JobName:                   ${job_name}"
  echo "Queue:                     ${queue}"
  echo "Reservation:               ${reservation}"
  echo "Num Nodes:                 ${num_nodes}"
  echo "Num Switches:              ${num_switches}"
  echo "GPUs per node:             ${gpus_per_node}"
  echo "Job dependency:            ${dependencyJob}"
  echo "Exec-Time:                 ${wc_limit}"

  # Display optional arguments
  if [ -z "${DISABLE_QARG_QOS}" ] || [ "${DISABLE_QARG_QOS}" == "false" ]; then
    echo "QoS:                       ${qos}"
  fi
  if [ "${ENABLE_PROJECT_NAME}" == "true" ]; then
    echo "Project name:              ${project_name}"
  fi
  if [ -z "${DISABLE_QARG_CONSTRAINTS}" ] || [ "${DISABLE_QARG_CONSTRAINTS}" == "false" ]; then
    echo "Constraints:               ${constraints}"
  fi

  # Display storage arguments
  echo "Storage Home:              ${storage_home}"
  echo "Storage Properties:        ${storage_props}"

  # Display arguments to runcompss
  local other
  other=$(echo "${args_pass}" | sed 's/\ --/\n\t\t\t--/g')
  echo "Other:                     $other"
  echo " "
}

###############################################
# Function that converts a cost in minutes
# to an expression of wall clock limit
###############################################
convert_to_wc() {
  local cost=$1
  wc_limit=${EMPTY_WC_LIMIT}

  local min=$((cost % 60))
  if [ $min -lt 10 ]; then
    wc_limit=":0${min}${wc_limit}"
  else
    wc_limit=":${min}${wc_limit}"
  fi

  local hrs=$((cost / 60))
  if [ $hrs -gt 0 ]; then
    if [ $hrs -lt 10 ]; then
      wc_limit="0${hrs}${wc_limit}"
    else
      wc_limit="${hrs}${wc_limit}"
    fi
  else
      wc_limit="00${wc_limit}"
  fi
}

#---------------------------------------------------
# MAIN FUNCTIONS DECLARATION
#---------------------------------------------------

###############################################
# Function to get the arguments
###############################################
get_args() {
  # Avoid enqueue if there is no application
  if [ $# -eq 0 ]; then
    usage 1
  fi

  #Parse COMPSs Options
  while getopts hvgtmd-: flag; do
    # Treat the argument
    case "$flag" in
      h)
        # Display help
        usage 0
        ;;
      v)
        # Display version
        display_version 0
        ;;
      -)
        # Check more complex arguments
        case "$OPTARG" in
          help)
            # Display help
            usage 0
            ;;
          version)
            # Display compss version
            display_version 0
            ;;
          opts)
            # Display options
            show_opts 0
            ;;
          sc_cfg=*)
            sc_cfg=${OPTARG//sc_cfg=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          job_name=*)
            job_name=${OPTARG//job_name=/}
            ;;
          master_working_dir=*)
            master_working_dir=${OPTARG//master_working_dir=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          exec_time=*)
            exec_time=${OPTARG//exec_time=/}
            ;;
          num_nodes=*)
            num_nodes=${OPTARG//num_nodes=/}
            ;;
          num_switches=*)
            num_switches=${OPTARG//num_switches=/}
            ;;
          cpus_per_node=*)
            cpus_per_node=${OPTARG//cpus_per_node=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          gpus_per_node=*)
            gpus_per_node=${OPTARG//gpus_per_node=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          queue=*)
            queue=${OPTARG//queue=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          reservation=*)
            reservation=${OPTARG//reservation=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          qos=*)
            qos=${OPTARG//qos=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          cpus_per_task)
            cpus_per_task="true"
            args_pass="$args_pass --$OPTARG"
            ;;
          constraints=*)
            constraints=${OPTARG//constraints=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          project_name=*)
            project_name=${OPTARG//project_name=/}
            ;;
          job_dependency=*)
            dependencyJob=${OPTARG//job_dependency=/}
            ;;
          node_memory=*)
            node_memory=${OPTARG//node_memory=/}
            ;;
          network=*)
            network=${OPTARG//network=/}
            args_pass="$args_pass --$OPTARG"
            ;;
          storage_home=*)
            storage_home=${OPTARG//storage_home=/}
            ;;
          storage_props=*)
            storage_props=${OPTARG//storage_props=/}
            ;;
          storage_conf=*)
            # Storage conf is automatically generated. Remove it from COMPSs flags
            echo "WARNING: storage_conf is automatically generated. Omitting parameter"
            ;;
          # Heterogeneous submission arguments
          types_cfg=*)
            # Used in heterogeneous_submit.sh
            # shellcheck disable=SC2034
            types_cfg_file=${OPTARG//types_cfg=/}
            ;;
          master=*)
            # Used in heterogeneous_submit.sh
            # shellcheck disable=SC2034
            master_type=${OPTARG//master=/}
            ;;
          workers=*)
            # Used in heterogeneous_submit.sh
            # shellcheck disable=SC2034
            worker_types=${OPTARG//workers=/}
            ;;
          uuid=*)
            echo "WARNING: uuid is automatically generated. Omitting parameter"
            ;;
          *)
            # Flag didn't match any patern. Add to COMPSs
            args_pass="$args_pass --$OPTARG"
            ;;
        esac
        ;;
      *)
        # Flag didn't match any patern. End of COMPSs flags
        args_pass="$args_pass -$flag"
        ;;
    esac
  done
  # Shift COMPSs arguments
  shift $((OPTIND-1))

  # Pass application name and args
  args_pass="$args_pass $*"
}

###############################################
# Function to set the wall clock time
###############################################
set_time() {
  if [ -z "${exec_time}" ]; then
    exec_time=${DEFAULT_EXEC_TIME}
  fi

  if [ -z "${WC_CONVERSION_FACTOR}" ]; then
    convert_to_wc "$exec_time"
  else
    wc_limit=$((exec_time * WC_CONVERSION_FACTOR))
  fi
}

###############################################
# Function to check the arguments
###############################################
check_args() {
  ###############################################################
  # Queue system checks
  ###############################################################
  if [ -z "${job_name}" ]; then
    job_name=${DEFAULT_JOB_NAME}
  fi

  if [ -z "${queue}" ]; then
    queue=${DEFAULT_QUEUE}
  fi

  if [ -z "${reservation}" ]; then
    reservation=${DEFAULT_RESERVATION}
  fi

  if [ -z "${constraints}" ]; then
    constraints=${DEFAULT_CONSTRAINTS}
  fi

  if [ -z "${qos}" ]; then
    qos=${DEFAULT_QOS}
  fi

  if [ -z "${dependencyJob}" ]; then
    dependencyJob=${DEFAULT_DEPENDENCY_JOB}
  fi

  ###############################################################
  # Infrastructure checks
  ###############################################################
  if [ -z "${num_nodes}" ]; then
    num_nodes=${DEFAULT_NUM_NODES}
  fi
  if [ "${num_nodes}" -lt "${MINIMUM_NUM_NODES}" ]; then
      display_error "${ERROR_NUM_NODES}" 1
  fi

  if [ -z "${num_switches}" ]; then
    num_switches=${DEFAULT_NUM_SWITCHES}
  fi

  if [ -z "${cpus_per_node}" ]; then
    cpus_per_node=${DEFAULT_CPUS_PER_NODE}
  fi

  if [ "${cpus_per_node}" -lt "${MINIMUM_CPUS_PER_NODE}" ]; then
    display_error "${ERROR_NUM_CPUS}"
  fi

  if [ -z "${gpus_per_node}" ]; then
    gpus_per_node=${DEFAULT_GPUS_PER_NODE}
  fi

  maxnodes=$((num_switches * MAX_NODES_SWITCH))

  if [ "${num_switches}" != "0" ] && [ "${maxnodes}" -lt "${num_nodes}" ]; then
    display_error "${ERROR_SWITCHES}"
  fi

  if [ "${num_nodes}" -lt "${MIN_NODES_REQ_SWITCH}" ] && [ "${num_switches}" != "0" ]; then
    display_error "${ERROR_NO_ASK_SWITCHES}"
  fi


  # Network variable and modification
  if [ -z "${network}" ]; then
    network=${DEFAULT_NETWORK}
  elif [ "${network}" == "default" ]; then
    network=${DEFAULT_NETWORK}
  elif [ "${network}" != "ethernet" ] && [ "${network}" != "infiniband" ] && [ "${network}" != "data" ]; then
    display_error "${ERROR_NETWORK}"
  fi

  ###############################################################
  # Node checks
  ###############################################################
  if [ -z "${node_memory}" ]; then
    node_memory=${DEFAULT_NODE_MEMORY}
  elif [ "${node_memory}" != "disabled" ] && ! [[ "${node_memory}" =~ ^[0-9]+$ ]]; then
    display_error "${ERROR_NODE_MEMORY}"
  fi

  if [ -z "${master_working_dir}" ]; then
    master_working_dir=${DEFAULT_MASTER_WORKING_DIR}
  fi

  ###############################################################
  # Storage checks
  ###############################################################
  if [ -z "${storage_home}" ]; then
    storage_home=${DEFAULT_STORAGE_HOME}
  fi

  if [ "${storage_home}" != "${DISABLED_STORAGE_HOME}" ]; then
    # Check storage props is defined
    if [ -z "${storage_props}" ]; then
      display_error "${ERROR_STORAGE_PROPS}"
    fi

    # Check storage props file exists
    if [ ! -f "${storage_props}" ]; then
      # PropsFile doesn't exist
      display_execution_error "${ERROR_STORAGE_PROPS_FILE}"
    fi
  fi

  ###############################################################
  # Project name check when required
  ###############################################################
  if [ "${ENABLE_PROJECT_NAME}" == "true" ] && [ -z "${project_name}" ]; then
    display_error "${ERROR_PROJECT_NAME_NA}"
  fi

}

###############################################
# Function to create a TMP submit script
###############################################
create_tmp_submit() {
  # Create TMP DIR for submit script
  TMP_SUBMIT_SCRIPT=$(mktemp)
  ev=$?
  if [ $ev -ne 0 ]; then
    display_error "${ERROR_TMP_FILE}" $ev
  fi
  echo "Temp submit script is: ${TMP_SUBMIT_SCRIPT}"

  cat > "${TMP_SUBMIT_SCRIPT}" << EOT
#!/bin/bash -e
#
EOT
}


###############################################
# MAIN ENTRY POINTS FROM SUBMIT/HETER_SUBMIT
###############################################

create_normal_tmp_submit(){
  create_tmp_submit
  add_submission_headers
  add_master_and_worker_nodes
  add_launch
}

add_submission_headers(){
  # Add queue selection
  if [ "${queue}" != "default" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_QUEUE_SELECTION}${QUEUE_SEPARATOR} ${queue}
EOT
  fi

  # Switches selection
  if [ -n "${QARG_NUM_SWITCHES}" ]; then
    if [ "${num_switches}" != "0" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_NUM_SWITCHES}${QUEUE_SEPARATOR}"cu[maxcus=${num_switches}]"
EOT
    fi
  fi

  # GPU selection
  if [ -n "${QARG_GPUS_PER_NODE}" ]; then
    if [ "${gpus_per_node}" != "0" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_GPUS_PER_NODE}${QUEUE_SEPARATOR}${gpus_per_node}
EOT
    fi
  fi

  # Add Job name and job dependency
  if [ "${dependencyJob}" != "None" ]; then
    if [ "${QARG_JOB_DEP_INLINE}" == "true" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}${job_name} ${QARG_JOB_DEPENDENCY_OPEN}${dependencyJob}${QARG_JOB_DEPENDENCY_CLOSE}
EOT
    else
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}${job_name}
#${QUEUE_CMD} ${QARG_JOB_DEPENDENCY_OPEN}${dependencyJob}${QARG_JOB_DEPENDENCY_CLOSE}
EOT
    fi
  else
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_NAME}${QUEUE_SEPARATOR}${job_name}
EOT
  fi

  # Reservation
  if [ -n "${QARG_RESERVATION}" ]; then
    if [ "${reservation}" != "disabled" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_RESERVATION}${QUEUE_SEPARATOR}${reservation}
EOT
    fi
  fi

  # QoS
  if [ -n "${QARG_QOS}" ]; then
    if [ "${qos}" != "default" ]; then
      if [ -z "${DISABLE_QARG_QOS}" ] || [ "${DISABLE_QARG_QOS}" == "false" ]; then
      	cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_QOS}${QUEUE_SEPARATOR}${qos}
EOT
      fi
    fi
  fi

  # QoS
  if [ -n "${QARG_OVERCOMMIT}" ]; then
      if [ -z "${DISABLE_QARG_OVERCOMMIT}" ] || [ "${DISABLE_QARG_OVERCOMMIT}" == "false" ]; then
        cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_OVERCOMMIT}
EOT
      fi
  fi

  # Constraints
  if [ -n "${QARG_CONSTRAINTS}" ]; then
    if [ "${constraints}" != "disabled" ]; then
      if [ -z "${DISABLE_QARG_CONSTRAINTS}" ] || [ "${DISABLE_QARG_CONSTRAINTS}" == "false" ]; then
        cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_CONSTRAINTS}${QUEUE_SEPARATOR}${constraints}
EOT
      fi
    fi
  fi

  # Node memory
  if [ -n "${QARG_MEMORY}" ]; then
    if [ "${node_memory}" != "disabled" ]; then
      if [ -z "${DISABLE_QARG_MEMORY}" ] || [ "${DISABLE_QARG_MEMORY}" == "false" ]; then
          cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_MEMORY}${QUEUE_SEPARATOR}${node_memory}
EOT
      fi
    fi
  fi

  # Add argument when exclusive mode is available
  if [ -n "${QARG_EXCLUSIVE_NODES}" ]; then
    if [ "${EXCLUSIVE_MODE}" != "disabled" ]; then
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_EXCLUSIVE_NODES}
EOT
    fi
  fi

  # Add argument when copy_env is defined
  if [ -n "${QARG_COPY_ENV}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_COPY_ENV}
EOT
  fi

  # Generic arguments
  cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_WALLCLOCK}${QUEUE_SEPARATOR}$wc_limit
#${QUEUE_CMD} ${QARG_WD}${QUEUE_SEPARATOR}${master_working_dir}
EOT
  # Add JOBID customizable stderr and stdout redirection when defined in queue system
  if [ -n "${QARG_JOB_OUT}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_OUT}${QUEUE_SEPARATOR} compss-${QJOB_ID}.out
EOT
  fi
  if [ -n "${QARG_JOB_ERROR}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_JOB_ERROR}${QUEUE_SEPARATOR} compss-${QJOB_ID}.err
EOT
  fi
  # Add num nodes when defined in queue system
  if [ -n "${QARG_NUM_NODES}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_NUM_NODES}${QUEUE_SEPARATOR}${num_nodes}
EOT
  fi

  # Add num processes when defined in queue system
  req_cpus_per_node=${cpus_per_node}
  if [ "${req_cpus_per_node}" -gt "${DEFAULT_CPUS_PER_NODE}" ]; then
    req_cpus_per_node=${DEFAULT_CPUS_PER_NODE}
  fi

  if [ -n "${QARG_NUM_PROCESSES}" ]; then
    if [ -n "${QNUM_PROCESSES_VALUE}" ]; then
      eval processes="${QNUM_PROCESSES_VALUE}"
    else
      processes=${req_cpus_per_node}
    fi
    echo "Requesting $processes processes"
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_NUM_PROCESSES}${QUEUE_SEPARATOR}${processes}
EOT
  fi

  # Span argument if defined on queue system
  if [ -n "${QARG_SPAN}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} $(eval "echo ${QARG_SPAN}")
EOT
  fi

  # Add project name defined in queue system
  if [ -n "${QARG_PROJECT_NAME}" ]; then
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_PROJECT_NAME}${QUEUE_SEPARATOR}${project_name}
EOT
  fi
}

add_packjob_separator(){
      cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
#${QUEUE_CMD} ${QARG_PACKJOB}
EOT
}

add_master_and_worker_nodes(){
  # Host list parsing
  cat >> "${TMP_SUBMIT_SCRIPT}" << EOT

  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${SCRIPT_DIR}/../../system/${HOSTLIST_CMD}"
  else
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST} ${HOSTLIST_TREATMENT})
    master_node=\$(${MASTER_NAME_CMD})
    worker_nodes=\$(echo \${host_list} | sed -e "s/\${master_node}//g")
  fi

EOT
}

add_only_master_node(){
  # Host list parsing
  cat >> "${TMP_SUBMIT_SCRIPT}" << EOT

  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${SCRIPT_DIR}/../../system/${HOSTLIST_CMD}"
  else
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST} ${HOSTLIST_TREATMENT})
    master_node=\$(${MASTER_NAME_CMD})
    worker_nodes=""
  fi

EOT
}

add_only_worker_nodes(){
 # Host list parsing
  local env_var_suffix=$1
  cat >> "$TMP_SUBMIT_SCRIPT" << EOT
  if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${SCRIPT_DIR}/../../system/${HOSTLIST_CMD}"
  else
    host_list=\$(${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST}${env_var_suffix} ${HOSTLIST_TREATMENT})
    worker_nodes=\$(echo \${host_list})
  fi

EOT
}

add_launch(){
  # Storage init
  if [ "${storage_home}" != "${DISABLED_STORAGE_HOME}" ]; then
    # ADD STORAGE_INIT, STORAGE_FINISH AND NODES PARSING
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT
storage_conf=$HOME/.COMPSs/\$${ENV_VAR_JOB_ID}/storage/cfgfiles/storage.properties
storage_master_node="\${master_node}"

${storage_home}/scripts/storage_init.sh \$${ENV_VAR_JOB_ID} "\${master_node}" "\${storage_master_node}" "\${worker_nodes}" ${network} ${storage_props}

${SCRIPT_DIR}/../../user/launch_compss --master_node="\${master_node}" --worker_nodes="\${worker_nodes}" --node_memory=${node_memory} --storage_conf=\${storage_conf} ${args_pass}

${storage_home}/scripts/storage_stop.sh \$${ENV_VAR_JOB_ID} "\${master_node}" "\${storage_master_node}" "\${worker_nodes}" ${network}

EOT
  else
    # ONLY ADD EXECUTE COMMAND
    cat >> "${TMP_SUBMIT_SCRIPT}" << EOT

${SCRIPT_DIR}/../../user/launch_compss --master_node="\${master_node}" --worker_nodes="\${worker_nodes}" --node_memory=${node_memory} ${args_pass}
EOT
  fi
}
