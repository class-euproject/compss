#!/bin/sh

charamount() {
    local STR=$1
    local CHAR=$2
    echo "$STR" | awk -F"$CHAR" '{print NF-1}'
}

error() {
    if [ -t 1 ]; then
        >&2 echo "\e[91m[ \e[5mERROR\e[25m ] $1\e[39m"
    else
        >&2 echo "[ ERROR ] $1"
    fi
    exit 1
}

IMAGE_NAME="$1"
IMAGE_ID="$2"
CONTAINER_NAME="$3"
PULL_IMAGE="$4"
CONTAINER_PORTS="$5"
PORT_RANGE="$6"
CONTAINER_VOLUMES="$7"
REPOSITORY="$8"
REUSE_EXISTING="$9"
FAIL_IF_PULL="${10}"
ENV_VARS="${11}"

shift 11
LAUNCH_COMMAND=""
for ARG in "$@"
do
    if [ -z "${LAUNCH_COMMAND}" ]; then
        LAUNCH_COMMAND="$ARG"
    else
        if [ "$ARG" != "$(echo ${ARG} | sed 's/ //g')" ]; then
            LAUNCH_COMMAND="${LAUNCH_COMMAND} \"$ARG\""
        else
            LAUNCH_COMMAND="${LAUNCH_COMMAND} $ARG"
        fi
    fi
done

DOCKER=`command -v docker`
if [ -z "${DOCKER}" ]; then
    warn "Command docker could not be found"
fi

DOCKER_CHECK_ERR="`${DOCKER} ps -a 2>&1 1> /dev/null`"
if [ $? -ne 0 ]; then
    error "User `whoami` on worker does not have permission to use Docker: \"${DOCKER_CHECK_ERR}\""
fi

if [ "${REPOSITORY}" = "null" ]; then
    unset REPOSITORY
fi

if [ "${CONTAINER_VOLUMES}" = "null" ]; then
    unset CONTAINER_VOLUMES
else
    for VOL_OP in `echo ${CONTAINER_VOLUMES} | sed -r 's/,/ /g'`; do
        VOLUME_ORIGIN=`echo $CONTAINER_VOLUMES | cut -d":" -f1`
        if [ ! -f ${VOLUME_ORIGIN} -a ! -d ${VOLUME_ORIGIN} ]; then
            DOCKER_VOLUMES=`${DOCKER} volume ls | grep ${VOLUME_ORIGIN}`
            if [ -z "${DOCKER_VOLUMES}" ]; then
                ${DOCKER} volume create ${VOLUME_ORIGIN}
            fi
        fi
    done
    CONTAINER_VOLUMES="-v `echo ${CONTAINER_VOLUMES} | sed -r 's/,/ -v /g'`"
fi

if [ "${ENV_VARS}" = "" ]; then
    unset ENV_VARS
else
    L_ENV_VARS=""
    for VAR in `echo ${ENV_VARS} | sed -r 's/,/ /g'`; do
        if [ -n "`echo ${VAR} | cut -d"=" -f1`" -a -n "`echo ${VAR} | cut -d"=" -f2`" ]; then
            L_ENV_VARS="-e ${VAR} "
        fi
    done
    ENV_VARS="${L_ENV_VARS}"
fi

IMAGE_LIST=$(${DOCKER} images --format "{{.Repository}}:{{.Tag}}" | grep "${IMAGE_NAME}")
if [ "$PULL_IMAGE" = "true" ]; then
    if [ "$IMAGE_ID" != "null" -a -z "$(${DOCKER} images --format "{{.ID}}" | grep ${IMAGE_ID})" ] || [ -z "${IMAGE_LIST}" ]; then
        echo "The image is not locally stored"
        if [ "${FAIL_IF_PULL}" = "true" ]; then
            mkdir -p /tmp/COMPSs/docker
            if [ -n "`ps aux | grep \"[d]ocker pull ${REPOSITORY}\(/\)\?${IMAGE_NAME}\"`" ]; then
                error "The image is still being pulled, due to the previous COMPSs execution or due to another user."
            fi
            if [ -f "/tmp/COMPSs/docker/$(echo ${IMAGE_NAME} | sed 's/\//_/g').out" ]; then
                if [ "OK" != "`tail -n 1 "/tmp/COMPSs/docker/$(echo ${IMAGE_NAME} | sed 's/\//_/g')".out`" ]; then
                    ERROR_MSG="`tail -n 1 /tmp/COMPSs/docker/$(echo ${IMAGE_NAME} | sed 's/\//_/g').out`"
                    rm "/tmp/COMPSs/docker/$(echo ${IMAGE_NAME} | sed 's/\//_/g')".out
                    error "There have been prior attempts to pull image ${IMAGE_NAME} from ${REPOSITORY-"the Docker Hub"} that have failed. Check the log: \"${ERROR_MSG}\". Try again after fixing."
                fi
            fi
            if [ -n "$REPOSITORY" ]; then
                /bin/sh -c "${DOCKER} pull \"${REPOSITORY}/${IMAGE_NAME}\" 2>&1 && ${DOCKER} tag \"${REPOSITORY}/${IMAGE_NAME}\" \"${IMAGE_NAME}\" 2>&1 && echo \"OK\"" 2>&1 > /tmp/COMPSs/docker/$(echo ${IMAGE_NAME} | sed 's/\//_/g').out &
            else
                /bin/sh -c "${DOCKER} pull \"${IMAGE_NAME}\" 2>&1 && echo \"OK\"" > /tmp/COMPSs/docker/$(echo ${IMAGE_NAME} | sed 's/\//_/g').out &
            fi
            error "The image is not available in the worker host. It is being pulled now, but the worker will be marked as failed."
        else
            if [ -n "$REPOSITORY" ]; then
                PULL_ERR=`${DOCKER} pull "${REPOSITORY}/${IMAGE_NAME}" 2>&1 1>/dev/null`
                if [ $? -ne 0 ]; then
                    error "An error happened when pulling image ${IMAGE_NAME} from the Docker Hub: ${PULL_ERR}"
                fi
                ${DOCKER} tag "${REPOSITORY}/${IMAGE_NAME}" "${IMAGE_NAME}"
                if [ $? -ne 0 ]; then
                    IMAGE_NAME="${REPOSITORY}/${IMAGE_NAME}"
                fi
            else
                PULL_ERR=`${DOCKER} pull "${IMAGE_NAME}" 2>&1 1>/dev/null`
                if [ $? -ne 0 ]; then
                    error "An error happened when pulling image ${IMAGE_NAME} from the Docker Hub: ${PULL_ERR}"
                fi
            fi
        fi
    fi
elif [ -z "${IMAGE_LIST}" ]; then
    error "The image is not locally stored and it is specified not to pull it"
fi
if [ "$IMAGE_ID" = "null" ]; then
    unset IMAGE_ID
fi

## Check if a container of the image already exists
# if [ "$REUSE_EXISTING" = "true" ]; then
#     CONTAINER_ID=$(docker ps --filter "ancestor=${IMAGE_NAME}" --format "{{.Names}}" | head -n 1)
#     if [ -n "${CONTAINER_ID}" ]; then
#         ${DOCKER} exec -t -d ${CONTAINER_NAME} /bin/sh -c "${LAUNCH_COMMAND}"
#         echo $(${DOCKER} port ${CONTAINER_ID} | cut -d"/" -f1 | xargs)
#         exit 0
#     fi
# fi

if [ "$PORT_RANGE" != "null" ]; then
    ALL_PORT_LIST=$(${DOCKER} ps --format "{{.Ports}}" | xargs)
    ALL_PORTS=""
    for P in ${ALL_PORT_LIST}; do
        P=$(echo "$P" | cut -d">" -f2 | cut -d"/" -f1)
        if [ $(charamount "$P" "-") -eq 1 ]; then
            ALL_PORTS="$ALL_PORTS $(seq `echo "$P" | cut -d"-" -f1` `echo "$P" | cut -d"-" -f2`)"
        else
            ALL_PORTS="$ALL_PORTS $P"
        fi
    done
    RANGE_INC_PORT=$(echo ${PORT_RANGE} | cut -d":" -f2)
    PORT_RANGE=$(echo ${PORT_RANGE} | cut -d":" -f1)
    INIT_PORT=$(echo ${PORT_RANGE} | cut -d"-" -f1)
    LAST_PORT=$(echo ${PORT_RANGE} | cut -d"-" -f2)
    # for P in &(seq $INIT_PORT $LAST_PORT)
    while [ ${INIT_PORT} -le ${LAST_PORT} ]; do
        PORT_IS_FREE=$(echo ${ALL_PORTS} | grep -c ${INIT_PORT}) # 0 if port is not in the list, thus free
        if [ ${PORT_IS_FREE} -eq 0 ]; then
            RANGE_PORT=${INIT_PORT}
            break
        fi
        INIT_PORT=$(($INIT_PORT + 1))
    done
    if [ ${INIT_PORT} -gt ${LAST_PORT} ]; then
        error "None of the ports specified with flag --range is available"
    elif [ "${CONTAINER_PORTS}" = "null" ]; then
        CONTAINER_PORTS="${RANGE_PORT}:${RANGE_INC_PORT}"
    else
        CONTAINER_PORTS="${CONTAINER_PORTS},${RANGE_PORT}:${RANGE_INC_PORT}"
    fi
fi

#if [ "${CONTAINER_PORTS}" != "null" ]; then
#    EXPOSED_PORTS=""
#    PORT_BINDINGS=""
#    for PORT in `echo "$CONTAINER_PORTS" | sed 's/,/ /g'`
#    do
#        HOST_PORT=$(echo "$PORT" | cut -d":" -f1)
#        CONTAINER_PORT=$(echo "$PORT" | cut -d":" -f2)
#        if [ -n "$EXPOSED_PORTS" ]; then
#            EXPOSED_PORTS="$EXPOSED_PORTS,"
#        fi
#        if [ -n "$PORT_BINDINGS" ]; then
#            PORT_BINDINGS="$PORT_BINDINGS,"
#        fi
#        EXPOSED_PORTS="$EXPOSED_PORTS \"$CONTAINER_PORT/tcp\": {}"
#        PORT_BINDINGS="$PORT_BINDINGS \"$CONTAINER_PORT/tcp\": [{\"HostPort\": \"$HOST_PORT\"}]"
#    done
#fi

# curl -XPOST \
#      --silent \
#      --fail \
#      -H 'Content-Type: application/json' \
#      --unix-socket /var/run/docker.sock \
#      -d"{
#         \"Tty\": true,
#         ${LAUNCH_COMMAND}
#         \"Image\": \"${IMAGE_ID-$IMAGE_NAME}\",
#         \"ExposedPorts\": {
#             $EXPOSED_PORTS
#         },
#         \"HostConfig\": {
#             \"PortBindings\": {
#                 $PORT_BINDINGS
#             },
#             \"Binds\": [ $CONTAINER_VOLUMES ]
#         }
#      }" \
#      http://v`${DOCKER} version --format "{{.Server.APIVersion}}"`/containers/create?name=${CONTAINER_NAME} > /dev/null
${DOCKER} run -d -t `echo ${CONTAINER_PORTS} | sed 's/,/ /g' | xargs printf -- "-p %s"` ${CONTAINER_VOLUMES} ${ENV_VARS} --name ${CONTAINER_NAME} ${IMAGE_NAME} /bin/sh -c "${LAUNCH_COMMAND}" > /dev/null &
#${DOCKER} exec -t -d ${CONTAINER_NAME} /bin/sh -c "${LAUNCH_COMMAND}"

CREATION_FAILED=$?
if [ ${CREATION_FAILED} -ne 0 ]; then
    error "The creation of the container failed"
fi

if [ "$PORT_RANGE" != null ]; then
    echo ${RANGE_PORT}
fi