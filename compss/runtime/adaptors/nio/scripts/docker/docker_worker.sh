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

pull() {
    if [ "$REPOSITORY" != "null" ]; then
        docker pull "${REPOSITORY}/${IMAGE_NAME}" > /dev/null
        docker tag "${REPOSITORY}/${IMAGE_NAME}" "${IMAGE_NAME}"
    else
        docker pull "${IMAGE_NAME}" > /dev/null
    fi
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

shift 9
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

if [ "${CONTAINER_VOLUMES}" = "null" ]; then
    unset CONTAINER_VOLUMES
else
    for VOL_OP in `echo ${CONTAINER_VOLUMES} | sed -r 's/,/ /g'`; do
        VOLUME_ORIGIN=`echo $CONTAINER_VOLUMES | cut -f":"`
        if [ ! -f ${VOLUME_ORIGIN} -a ! -d ${VOLUME_ORIGIN} ]; then
            DOCKER_VOLUMES=`docker volume ls | grep ${VOLUME_ORIGIN}`
            if [ -z "${DOCKER_VOLUMES}" ]; then
                docker volume create ${VOLUME_ORIGIN}
            fi
        fi
    done
    CONTAINER_VOLUMES="-v `echo ${CONTAINER_VOLUMES} | sed -r 's/,/ -v /g'`"
fi

IMAGE_LIST=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "${IMAGE_NAME}")
if [ "$PULL_IMAGE" = "true" ]; then
    if [ "$IMAGE_ID" != "null" -a -z "$(docker images --format "{{.ID}}" | grep ${IMAGE_ID})" ] || [ -z "${IMAGE_LIST}" ]; then
        pull
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
#         docker exec -t -d ${CONTAINER_NAME} /bin/sh -c "${LAUNCH_COMMAND}"
#         echo $(docker port ${CONTAINER_ID} | cut -d"/" -f1 | xargs)
#         exit 0
#     fi
# fi

if [ "$PORT_RANGE" != "null" ]; then
    ALL_PORT_LIST=$(docker ps --format "{{.Ports}}" | xargs)
    ALL_PORTS=""
    for P in ${ALL_PORT_LIST}; do
        echo "P BEFORE IS $P"
        P=$(echo "$P" | cut -d">" -f2 | cut -d"/" -f1)
        echo "P AFTER IS $P"
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
#      http://v`docker version --format "{{.Server.APIVersion}}"`/containers/create?name=${CONTAINER_NAME} > /dev/null
docker run -d -t `echo ${CONTAINER_PORTS} | sed 's/,/ /g' | xargs printf -- "-p %s"` ${CONTAINER_VOLUMES} --name ${CONTAINER_NAME} ${IMAGE_NAME} /bin/sh -c "${LAUNCH_COMMAND}" > /dev/null
#docker exec -t -d ${CONTAINER_NAME} /bin/sh -c "${LAUNCH_COMMAND}"
CREATION_FAILED=$?
if [ ${CREATION_FAILED} -ne 0 ]; then
    error "The creation of the container failed"
    exit ${CREATION_FAILED}
fi

docker start ${CONTAINER_NAME} > /dev/null
START_FAILED=$?
if [ ${START_FAILED} -ne 0 ]; then
    error "Container could not be started."
    exit ${START_FAILED}
fi

if [ "$PORT_RANGE" != null ]; then
    echo ${RANGE_PORT}
fi