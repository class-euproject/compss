#!/bin/sh

usage() {
    cat <<EOF
    Linux Containers (LXC) distribution tool.

    Mandatory flags:

    -h, --help          Shows this help message.

    -i, --image         Sets the name of the image to launch. If it starts with "<name>:",
                        it will be assumed to be in repository "<name>".

    -w, --worker        The IP to use to connect through SSH to the worker.

    -m, --master        The hostname, FQDN, or IP to be used in the worker to identify the
                        master.

    Optional flags:

    -p,                 The script will add the master as a public server on the agent.
    --as-public-server

    --rollback-image    The script will make the image private after the transmission
                        if the flag --as-public-server has been used.

    -n, --name          Sets the name of the container.

    -c, --cert          Path to the certificate to add if --as-public-server has
                        not been used (default: /var/snap/lxd/common/lxd/server.crt).

    --master-name       Name with which refer to the master as a remote in the
                        worker. (default: \$MASTER_ADDRESS)

    -u, --user          The username with which to connect to the worker through SSH.

    --pull              If the image is not in the remote worker, copy it from the
                        master.

    --ports             Ports to open from the container to the worker host,
                        following format: <HOST_PORT>:<CONTAINER_PORT>,
                        <HOST_PORT2>:<CONTAINER_PORT2>,...

    --range             Range of ports, separated by a hyphen, from which to choose
                        only ONE for the port forwarding. This port inside the
                        container will forward to the same port in the host, unless
                        one is explicitly specified after a colon (:). For example, if
                        --range 40000-40010 is set, availability of all ports from
                        40000 to 40010 will be checked. Otherwise, if --range
                        40000-40010:80 is set, the port chosen from the range will
                        forward to the port 80 inside the container. The first
                        available port will be used and printed to STDOUT. If --reuse
                        has been set, and a container of the image is already running,
                        no forwarding can be added. If any forwarding already exists,
                        those ports will be printed.

    --storage           Sets the storage settings, following format:
                        <STORAGE_POOL>:<VOLUME_NAME>:<MOUNTING_POINT>. Only supports
                        "dir" volumes.

    --                  After this flag, the execution command of the container must be set.

    Regarding image transmission, when it comes to remotely launching a LXC container there are two options. Both assume the image is in the host in which this script is run, this is, the master.

    The first option is to make the needed image public, so that any other host that adds the master as a remote public server can access its public images, then download them. This procedure does not need any certificate file for the authentication. The procedure can be run with this script by running:

    ./master.sh --as-public-server --image <image> --master <master> --worker <worker> -- <command>

    which would roughly translate to running these commands in the master:

    lxc image edit <image> <<EOF
    public: true
    EOF

    ...and these others in the worker:

    lxc remote add <master> <master> --public --accept-certificate
    lxc image copy <master>:<image> local: --alias <image>
    lxc launch ...

    After its usage, this image remains public, unless explicitly made private again. This can result in unwanted and uncontrolled usage of the image. To avoid this, the flag --rollback-image can be added when running this script:

    ./master.sh --as-public-server [...] --rollback-image

    which translates to the following command run in the master:

    lxc image edit <image> <<EOF
    public: false
    EOF

    The second option is to add the master as a trusted remote server on the worker. To do so, the certificate of the LXC server running on the master is needed, usually located in /var/snap/lxd/common/lxd/server.crt (and usually requiring root privileges to read). This option can be run with this script by running:

    ./master.sh --image <image> --master <master> --worker <worker> --cert <cert path> -- <command>

    which would translate to running these commands in the worker (after sending the certificate to it):

    lxc config trust add <cert path>
    lxc remote add <master> <master> --accept-certificate
    lxc launch ...

EOF
}


error() {
    if [ -t 1 ]; then
        >&2 echo "\e[91m[ \e[5mERROR\e[25m ] $1\e[39m"
    else
        >&2 echo "[ ERROR ] $1"
    fi
    exit 1
}

warn() {
    if [ -t 1 ]; then
        echo "\e[33m[\e[5mWARNING\e[25m] $1\e[39m"
    else
        echo "[WARNING] $1"
    fi
}

charamount() {
    local STR=$1
    local CHAR=$2
    echo "$STR" | awk -F"$CHAR" '{print NF-1}'
}

DEFAULT_CERT_FILE='$HOME/snap/lxd/current/.config/lxc/client.crt'
AS_PUBLIC_SERVER=false
ROLLBACK_IMAGE=false
PULL_IMAGE=false
while [ "$1" != "" ]; do
    case $1 in
        -h | --help)
            usage
            exit;;
        -n | --name)
            CONTAINER_NAME="$2"
            shift;;
        -i | --image)
            IMAGE_NAME="$2"
            shift;;
        -p | --as-public-server)
            AS_PUBLIC_SERVER=true;;
        --rollback-image)
            ROLLBACK_IMAGE=true;;
        -c | --cert)
            CERT_FILE="$2"
            shift;;
        -m | --master)
            MASTER_ADDRESS="$2"
            shift;;
        --master-name)
            REMOTE_NAME="$2"
            shift;;
        -w | --worker)
            WORKER_NAME="$2"
            shift;;
        -u | --user)
            WORKER_USER="$2"
            shift;;
        --pull)
            PULL_IMAGE=true;;
        --ports)
            CONTAINER_PORTS="$2"
            shift;;
        --storage)
            CONTAINER_VOLUMES="$2"
            shift;;
        --range)
            PORT_RANGE="$2"
            shift;;
        --)
            shift
            break;;
        *)
        error "The flag $1 is unknown. Check out for typos!"
        exit;;
    esac
    shift
done
LAUNCH_COMMAND=""
for ARG in "$@"
do
    if [ "$ARG" != "$(echo $ARG | sed 's/ //g')" ]; then
        LAUNCH_COMMAND="${LAUNCH_COMMAND} \"$ARG\""
    else
        LAUNCH_COMMAND="${LAUNCH_COMMAND} $ARG"
    fi
done

if [ -z "${IMAGE_NAME}" ]; then
    error "The image name MUST be declared"
fi

USES_REMOTE=false
if [ `charamount "${IMAGE_NAME}" ":"` -eq "1" ]; then
    warn "The master will not be used as the image's origin"
    USES_REMOTE=true
    REMOTE_NAME="`echo ${IMAGE_NAME} | cut -d':' -f1`"
    IMAGE_NAME="`echo ${IMAGE_NAME} | cut -d':' -f2`"
elif [ -z "${MASTER_ADDRESS}" ]; then
    warn "Neither the master address or the remote name have been set. The image is assumed to be in the worker"
    if [ "${PULL_IMAGE}" = "true" ]; then
        error "If no master address or remote name is provided, can not force a pull"
    fi
fi

if [ -z "${WORKER_NAME}" ]; then
    error "The worker IP MUST be declared"
elif [ -z `echo "${WORKER_NAME}" | grep -E "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"` ]; then
    error "The worker IP is not really an IP"
fi

LXC=$(command -v lxc)
if [ -z "${LXC}" ]; then
    warn "Command LXC does not exist"
    if [ "$USES_REMOTE" = "false" ]; then
        error "If command LXC does not exist then image must be in a remote server, and should be set as <remote>:<image name>"
    fi
fi

if [ "$AS_PUBLIC_SERVER" = "true" -a "$ROLLBACK_IMAGE" = "false" ]; then
    warn "You chose to make the image public, but you chose not to make it private back again. Are you sure? Press CTRL+C to cancel."
fi
if [ "$AS_PUBLIC_SERVER" = "true" -a -n "${CERT_FILE}" ]; then
    warn "Both --as-public-server and --cert were specified. The public server option will prevail. Press CTRL+C to cancel."
fi
if [ "$AS_PUBLIC_SERVER" = "false" -a -z "${CERT_FILE}" ]; then
    warn "If the flag --as-public-server is not used, a certificate file should be declared. This will fall back to the default $DEFAULT_CERT_FILE."
fi
if [ "$AS_PUBLIC_SERVER" = "true" -o -n "${CERT_FILE}" ]; then
    warn "Image transmission has been set up via public server or certificate file. The image pull will be forced."
    PULL_IMAGE="true"
fi
if [ -z "$CONTAINER_NAME" ]; then
    CONTAINER_NAME="$(echo ${IMAGE_NAME} | cut -d"/" -f1)-$(uuidgen | cut -d"-" -f1)"
    warn "Container name not explicitly defined, set to ${CONTAINER_NAME}."
fi

if [ -n "${CONTAINER_VOLUMES}" ]; then
    if [ `charamount ${CONTAINER_VOLUMES} ":"` -ne 2 ]; then
        error "The specified container volume setting does not follow the correct format"
    fi
fi

if [ -z "${WORKER_USER}" ]; then
    WORKER_USER=`whoami`
fi

if [ "$USES_REMOTE" = "false" ]; then
    IMAGE_FINGERPRINT=`lxc image ls -cLF | grep -w " ${IMAGE_NAME} " | sed 's/\s//g' | cut -d"|" -f3`

    if [ -z "${IMAGE_FINGERPRINT}" ]; then
        error "The image does not locally exist or there has been an error"
    fi

    if [ "$AS_PUBLIC_SERVER" = "false" ]; then
        TMP_CERT_DIR=`mktemp -d`
        echo "`ssh -o "BatchMode yes" -o StrictHostKeychecking=no ${WORKER_USER}@${WORKER_NAME} cat ${CERT_FILE-$DEFAULT_CERT_FILE}`" > $TMP_CERT_DIR/cert.crt
        lxc config trust add $TMP_CERT_DIR/cert.crt
        rm -rf TMP_CERT_DIR
    else
        lxc image edit ${IMAGE_FINGERPRINT} <<EOF
public: true
EOF
    fi
fi

cat <<EOF
Parameters:
    Container's name: $CONTAINER_NAME
    Container ports: ${CONTAINER_PORTS:-none}
    Container image: $IMAGE_NAME
    Worker's hostname: $WORKER_NAME
    Force image pull if does not exist: $PULL_IMAGE
EOF
if [ -n "${CONTAINER_VOLUMES}" ]; then
    cat <<EOF
    Storage settings:
        Storage pool: $(echo ${CONTAINER_VOLUMES} | cut -d":" -f1)
        Volume: $(echo ${CONTAINER_VOLUMES} | cut -d":" -f2)
        Mount point: $(echo ${CONTAINER_VOLUMES} | cut -d":" -f3)
EOF
fi
if [ "$USES_REMOTE" = "true" ]; then
    echo "    Using remote: $REMOTE_NAME"
else
    echo "    Using master as remote server."
    echo "        With name: ${REMOTE_NAME-master}"
    echo "        With address: $MASTER_ADDRESS"
    echo "        As public server: $( test $AS_PUBLIC_SERVER = true && echo yes || echo no )"
fi
if [ -z "$LAUNCH_COMMAND" ]; then
    echo "    No execution command defined"
else 
    echo "    Run command for container: $LAUNCH_COMMAND"
fi

if [ -n "${PORT_RANGE}" ]; then
    if [ $(charamount ${PORT_RANGE} "-") -ne 1 -a $(charamount ${PORT_RANGE} ":") -ne 1 ]; then
        error "If you set the range flag, you must specify both the range of outside ports, and the inside port to which redirect"
    fi
fi

ssh -o BatchMode=yes -o StrictHostKeyChecking=no ${WORKER_USER}@${WORKER_NAME} "/bin/sh -s" < ${COMPSS_HOME:-/opt/COMPSs}/Runtime/scripts/system/adaptors/nio/lxc/lxc_worker.sh "${CONTAINER_NAME}" "${CONTAINER_PORTS:-null}" "${IMAGE_NAME}" "${PULL_IMAGE}" "${USES_REMOTE}" "${REMOTE_NAME:-null}" "${MASTER_ADDRESS:-null}" "${WORKER_NAME}" "${AS_PUBLIC_SERVER}" "${CONTAINER_VOLUMES:-null}" "${PORT_RANGE-null}" "${LAUNCH_COMMAND}"

if [ "$?" -gt 0 ]; then
    error "An error happened. Be sure to check out the output"
fi

if [ "$ROLLBACK_IMAGE" = "true" ]; then
    lxc image edit ${IMAGE_FINGERPRINT} <<EOF
public: false
EOF
fi