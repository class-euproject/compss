#!/bin/sh
OK() {
    if [ -t 1 ]; then
        echo "\e[92m$1\e[39m"
    else
        echo "[  OK!  ] $1"
    fi
}

error() {
    if [ -t 1 ]; then
        >&2 echo "\e[91m[ \e[5mERROR\e[25m ] $1\e[39m"
    else
        >&2 echo "[ ERROR ] $1"
    fi
    exit 1
}

CONTAINER_NAME=$1
CONTAINER_PORTS=$2
if [ "$CONTAINER_PORTS" = "null" ]; then
    CONTAINER_PORTS=""
fi
IMAGE_NAME=$3
COPY_IMAGE=$4
USES_REMOTE=$5
REMOTE_NAME=$6
MASTER_ADDRESS=$7
HOST_ADDRESS=$8
AS_PUBLIC_SERVER=$9
CONTAINER_VOLUMES=$10

shift 10
LAUNCH_COMMAND=""
for ARG in "$@"
do
    #if [ "$ARG" != "$(echo $ARG | sed 's/ //g')" ]; then
        #LAUNCH_COMMAND="${LAUNCH_COMMAND} \\\"$ARG\\\""
    #else
    if [ -z "${LAUNCH_COMMAND}" ]; then
        LAUNCH_COMMAND="$ARG"
    else
        #if [ "$ARG" != "$(echo $ARG | sed 's/ //g')" ]; then
        #    LAUNCH_COMMAND="${LAUNCH_COMMAND}\", \"\\\"$ARG\\\""
        #else
            LAUNCH_COMMAND="${LAUNCH_COMMAND}\", \"$ARG"
        #fi
    fi
    #fi
done
# LAUNCH_COMMAND="${LAUNCH_COMMAND}\", \">\", \"log.out"

COMMAND_OUTPUT=`command -v lxc`
if [ ! -f "/snap/bin/lxc" -a -z "${COMMAND_OUTPUT}" ]; then
    error "LXC could not be found. It might not be installed, or it might not be in the path. If the last is the case, add it into the path in /etc/environment"
else
    if [ -f "/snap/bin/lxc" ]; then
        PATH=$PATH:/snap/bin
    else
        PATH=$PATH:`${COMMAND_OUTPUT} | cut -d"/" -f-$(charamount $COMMAND_OUTPUT "/")`
    fi
fi

LXC_IMAGE_LIST=`lxc image ls -cL | grep -w "$(echo $IMAGE_NAME | cut -d":" -f2)"`
if [ -z "${LXC_IMAGE_LIST}" ]; then
    echo "The image does not exist"
    if [ "$COPY_IMAGE" = "false" -o -z "${MASTER_ADDRESS}" -a -z "${REMOTE_NAME}" ]; then
        error "Image pull can not forced. Exiting."
        exit 1
    fi
    LXC_REMOTE_LIST=`lxc remote ls | grep -w "${REMOTE_NAME}"`
    if [ -z "${LXC_REMOTE_LIST}" ]; then
        echo "The remote does not exist"
        if [ "$USES_REMOTE" = "true" ]; then
            error "The remote is not the master server and is unknown for the worker. Exiting."
            exit 1
        else
            if [ "$AS_PUBLIC_SERVER" = "true" ]; then
                echo "Adding master as public server..."
                lxc remote add $REMOTE_NAME $MASTER_ADDRESS --accept-certificate --public
                OK "Master added as public server"
            else
                echo "Adding master as private server..."
                lxc remote add $REMOTE_NAME $MASTER_ADDRESS --accept-certificate
                OK "Master added as private server"
            fi
        fi
    else
        OK "The remote already exists"
    fi
    echo "Copying image..."
    echo lxc image copy ${REMOTE_NAME}:${IMAGE_NAME} local: --alias ${IMAGE_NAME}
    lxc image copy ${REMOTE_NAME}:${IMAGE_NAME} local: --alias ${IMAGE_NAME}
    OK "Image copied."
    if [ "$AS_PUBLIC_SERVER" = "true" ]; then
        echo "Removing master from remote list..."
        #lxc remote rm $REMOTE_NAME
        OK "Master removed from remote list"
    fi
else
    OK "The image already exists"
fi

if [ "${CONTAINER_VOLUMES}" != "null" ]; then
    echo "CONTAINER_VOLUMES is ${CONTAINER_VOLUMES}"
    ST_POOL=`echo ${CONTAINER_VOLUMES} | cut -d":" -f1`
    ST_VOL=`echo ${CONTAINER_VOLUMES} | cut -d":" -f2`
    VOL_MOUNT_PATH=`echo ${CONTAINER_VOLUMES} | cut -d":" -f3`
    if [ -z "`lxc storage ls | grep ${ST_POOL}`" ]; then
        lxc storage create ${ST_POOL} dir
        lxc storage volume create ${ST_POOL} ${ST_VOL}
    elif [ -z "`lxc storage volume ls ${ST_POOL}`" ]; then
        lxc storage volume create ${ST_POOL} ${ST_VOL}
    fi
    LAUNCH_VOL="-s ${ST_POOL}"
fi

# LXC_PROFILES=`lxc profile ls | grep compss`
# if [ -z ${LXC_PROFILES} ]; then
#     lxc profile copy default compss
#     lxc profile set compss environment.TZ `cat /etc/timezone`
# fi

echo "Launching container..."
lxc launch ${LAUNCH_VOL} ${IMAGE_NAME} ${CONTAINER_NAME}
if [ $? -ne 0 ]; then
    error "An error happened when launching the container."
fi
OK "Container launched."

if [ "${CONTAINER_VOLUMES}" != "null" ]; then
    echo "Attaching volume to container"
    lxc storage volume attach ${ST_POOL} ${ST_VOL} ${CONTAINER_NAME} ${VOL_MOUNT_PATH}
else
    echo "Not attaching any volume to the container"
fi

if [ "${CONTAINER_PORTS}" = "" ]; then
    echo "No port forwarding will be added."
fi
for PORTS in $(echo ${CONTAINER_PORTS} | sed 's/,/ /g')
do
    HOST_PORT=$(echo ${PORTS} | cut -d":" -f1)
    CONTAINER_PORT=$(echo ${PORTS} | cut -d":" -f2)
    echo "Adding port forwarding from host's ${HOST_PORT} to container's ${CONTAINER_PORT}"
    lxc config device add ${CONTAINER_NAME} "${CONTAINER_NAME}-${CONTAINER_PORT}" proxy listen=tcp:${HOST_ADDRESS}:${HOST_PORT} connect=tcp:127.0.0.1:${CONTAINER_PORT}
    OK "Port forwarding from ${HOST_PORT} to ${CONTAINER_PORT} added"
done

if [ -n "${LAUNCH_COMMAND}" ]; then
    echo "Running execution command inside the container..."
    curl --silent --fail -XPOST --unix-socket /var/snap/lxd/common/lxd/unix.socket http://1.0/1.0/containers/${CONTAINER_NAME}/exec -d"{
        \"command\": [\"$LAUNCH_COMMAND\"],
        \"environment\": {},
        \"wait-for-websocket\": false,
        \"record-output\": true,
        \"interactive\": true
    }" >/dev/null
    if [ $? -gt 0 ]; then
        error "Execution command was not run successfully"
    else
        OK "Execution command run successfully"
    fi
fi