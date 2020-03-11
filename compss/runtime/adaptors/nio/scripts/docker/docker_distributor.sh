#!/bin/sh

usage() {
    cat <<EOF
    Docker distribution tool. Available flags:

        Mandatory:
    --image         Image to run. This must be in the following format:
                    [<repo url>[:<port>]/][<user name>/]<image name>[:<tag>]
                    If an image with a repository URL is defined, it will
                    automatically be parsed and used for pushing (if set)

    --worker        Address of the worker in which to distribute the container.
                    Can be an IP or a hostname, as long as it is reachable and
                    it is possible to connect via passwordless SSH.

        Optional:

    --name          Unique name to give to the container (in case a new container
                    needs to be created). A random one will be automatically
                    generated if not set.

    --user          User with which to connect via SSH to the worker. Set by
                    default to "`whoami`".

    --pull          Force the worker to pull the image if it is not available.
                    The image will be pulled following Docker engine's guidelines,
                    if the image's name does not refer to a repository or no
                    repository has been defined, it will be pulled from the
                    Docker Hub.

    --push          If the image is stored locally and not in the intended
                    repository, and either the image's name refers to a repository
                    or the repository has been specified, the flag will force
                    the upload prior to the distribution. If the image is forced
                    to be pushed, it will also be force-pulled in the worker.

    --repository    If the image does not currently refer to a repository, it can
                    be set using this flag. Only useful for pushing.

    --ports         List of ports to expose and forward to the container,
                    following the format "<host port>:<container port>", delimited
                    by commas. They are assumed to be used for TCP communications.

    --range         Range of ports, separated by a hyphen, from which to choose
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

    --volumes       List of directory bindings delimited by commas. This is, list
                    of directories of the host that will bind to the list of
                    container directories. The format is: <host dir>:<cont. dir>,...
                    This will also work with volumes.

    --env           Environmental variable to set inside the container.

    --              After this flag, the input will not be parsed and it will be
                    considered as the command with which to initialize the
                    container.

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

PULL_IMAGE=false
PUSH_IMAGE=false
FAIL_IF_PULL=false
SSH_USER=`whoami`
REUSE_EXISTING=false
CHECK_IMAGE=true
ENV_VARS=""
while [ "$1" != "" ]; do
    case $1 in
        -h | --help)
            usage
            exit;;
        --image)
            IMAGE_NAME="$2"
            shift;;
        --name)
            CONTAINER_NAME="$2"
            shift;;
        --pull)
            PULL_IMAGE=true;;
        --fail-if-pull)
            FAIL_IF_PULL=true;;
        --push)
            PUSH_IMAGE=true
            PULL_IMAGE=true;;
        --ports)
            CONTAINER_PORTS="$2"
            shift;;
        --repository)
            REPOSITORY="$2"
            shift;;
        --user)
            SSH_USER="$2"
            shift;;
        --worker)
            WORKER_ADDRESS="$2"
            shift;;
        --volumes)
            CONTAINER_VOLUMES="$2"
            shift;;
        --reuse)
            REUSE_EXISTING=true;;
        --range)
            PORT_RANGE="$2"
            shift;;
        --env)
            if [ -n "${ENV_VARS}" -a "${ENV_VARS}" != "" ]; then
                ENV_VARS="${ENV_VARS},"
            fi
            ENV_VARS="${ENV_VARS}$2"
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

DOCKER=`command -v docker`
if [ -z "${DOCKER}" ]; then
    warn "Command docker could not be found"
fi

LAUNCH_COMMAND=""
for ARG in "$@"
do
    if [ "$ARG" != "$(echo ${ARG} | sed 's/ //g')" ]; then
        LAUNCH_COMMAND="${LAUNCH_COMMAND} \"$ARG\""
    else
        LAUNCH_COMMAND="${LAUNCH_COMMAND} $ARG"
    fi
done

if [ -z "${IMAGE_NAME}" ]; then
    error "The image name MUST be declared"
fi

if [ -z "${WORKER_ADDRESS}" ]; then
    error "The worker's address MUST be specified"
fi

if [ -z "${CONTAINER_NAME}" ]; then
    SLASH_AMOUNT=$(charamount "${IMAGE_NAME}" "/")
    CONTAINER_NAME="$(echo ${IMAGE_NAME} | cut -d"/" -f$(($SLASH_AMOUNT + 1)) | cut -d":" -f1)-$(uuidgen | cut -d"-" -f1)"
    warn "The container name was not defined. Automatically defined to ${CONTAINER_NAME}"
fi

if [ "$PUSH_IMAGE" = "true" ]; then
    if [ -z "${DOCKER}" ]; then
        error "To push an image, the docker CLI needs to be installed."
    fi
    if [ `charamount "${IMAGE_NAME}" "/"` -eq 2 -a -z ${REPOSITORY} ]; then
        REPOSITORY=`echo "${IMAGE_NAME}" | cut -d"/" -f1`
        IMAGE_NAME=`echo ${IMAGE_NAME} | cut -d"/" -f2-`
    fi
    if [ `charamount ${IMAGE_NAME} ":"` -eq 1 ]; then
        IMAGE_TAG=`echo ${IMAGE_NAME} | cut -d":" -f2`
        IMAGE_NAME=`echo ${IMAGE_NAME} | cut -d":" -f1`
    fi
    if [ -z "$REPOSITORY" ]; then
        warn "The image has to be pushed, but no repository was defined. It will be pushed to the DockerHub."
        echo "curl --silent --fail -lSL https://index.docker.io/v1/repositories/${IMAGE_NAME}/tags/${IMAGE_TAG:-latest}"
        curl --silent --fail -lSL https://index.docker.io/v1/repositories/${IMAGE_NAME}/tags/${IMAGE_TAG:-latest}
        if [ $? -ne 0 ]; then
            ${DOCKER} push ${IMAGE_NAME}
            PUSH_FAILED=$?
            if [ ${PUSH_FAILED} -ne 0 ]; then
                warn "The image could not be pushed. The repository might be down."
                warn "The execution will carry on, assuming the images might already be in the worker."
            fi
        else
            warn "The image was already pushed, or another image with the same already existed."
        fi
    else
        # CLEAN_IMAGE_NAME=$(echo "$IMAGE_NAME" | cut -d: -f1)
        # CLEAN_IMAGE_TAG=$(echo "$IMAGE_NAME" | cut -d: -f2)
        REPO_OUTPUT=$(curl --fail --silent ${REPOSITORY}/v2/${IMAGE_NAME}/tags/list)
        REPO_TAGS=$(echo ${REPO_OUTPUT} | cut -b$(($(echo ${REPO_OUTPUT} | grep -bo "tags\"\:\[" | sed 's/:.*$//') + 8))- | cut -d] -f1)
        if [ $(echo ${REPO_TAGS} | grep -q ${IMAGE_TAG:-latest} && echo true || echo false) = "false" ]; then
            warn "The image is not available in the repository. Pushing."
            ${DOCKER} tag ${IMAGE_NAME} ${REPOSITORY}/${IMAGE_NAME}
            ${DOCKER} push ${REPOSITORY}/${IMAGE_NAME}
            PUSH_FAILED=$?
            if [ ${PUSH_FAILED} -ne 0 ]; then
                warn "The image could not be pushed. The repository might be down."
                warn "The execution will carry on, assuming the images might already be in the worker."
            fi
        fi
    fi
    IMAGE_ID=$(${DOCKER} images --format "{{.ID}}" ${IMAGE_NAME})
elif [ "${PULL_IMAGE}" = "true" ]; then
    warn "The image will be pulled in the worker, but will not be pushed from here. Make sure it is already in the repository or in DockerHub."
fi

if [ -n "${PORT_RANGE}" ]; then
    if [ $(charamount ${PORT_RANGE} "-") -ne 1 -a $(charamount ${PORT_RANGE} ":") -ne 1 ]; then
        error "If you set the range flag, you must specify both the range of outside ports, and the inside port to which redirect"
    fi
fi

ssh -o BatchMode=yes -o StrictHostKeyChecking=no ${SSH_USER}@${WORKER_ADDRESS} "/bin/sh -s" < ${COMPSS_HOME:-/opt/COMPSs}/Runtime/scripts/system/adaptors/nio/docker/docker_worker.sh "${IMAGE_NAME}" "${IMAGE_ID-null}" "${CONTAINER_NAME}" "${PULL_IMAGE}" "${CONTAINER_PORTS:-null}" "${PORT_RANGE:-null}" "${CONTAINER_VOLUMES:-null}" "${REPOSITORY:-null}" "${REUSE_EXISTING}" "${FAIL_IF_PULL}" "${ENV_VARS:-null}" "${LAUNCH_COMMAND}"