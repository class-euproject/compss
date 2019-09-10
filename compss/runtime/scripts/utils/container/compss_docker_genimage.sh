#!/bin/sh

error() {
    >&2 echo "\e[91m[ \e[5mERROR\e[25m ] $1\e[39m"
}

warn() {
    echo "\e[33m[\e[5mWARNING\e[25m] $1\e[39m"
}

charamount() {
    local STR=$1
    local CHAR=$2
    echo "$STR" | awk -F"$CHAR" '{print NF-1}'
}

arch_trans() {
    if [ -z "$1" ]; then
        # No input
        echo "Input must be provided"
        exit 1
    # Supported architectures
    elif [ "$1" = "x86_64" -o "$1" = "amd64" ]; then
        echo "amd64"
    elif [ "$1" = "armv7l" ]; then
        echo "arm32v7"
    # Unsupported architectures
    else
        echo "Unsupported architecture detected"
        exit 1
    fi
}

DEFAULT_BASE_TYPE=compss
DEFAULT_ARCH=$(arch_trans `arch`)
DEFAULT_WORKER_TYPE=nio
DEFAULT_COMPSS_VERSION=2.5.rc1907
DEFAULT_APP_DIR=.
DEFAULT_WD=/compss

usage() {
    cat <<EOF
Script for the creation of a COMPSs application Docker image. Available flags:

Mandatory flags:
    --name [name]       Name to give to the created image.

Optional flags:
    --base [image]      Full name of the base image to be used.
                Format: [<repository>/][<username>/]<image name>[:<tag>]
        or
    --version [version] Version of COMPSs to use. If this is specified, the name
                        of the base image will automatically fall back to:
                        ${DEFAULT_BASE_TYPE}/${DEFAULT_WORKER_TYPE}-worker-${DEFAULT_ARCH}:\$VERSION

            If neither are used, the used image will automatically fall back to:
              ${DEFAULT_BASE_TYPE}/${DEFAULT_WORKER_TYPE}-worker-${DEFAULT_ARCH}:${DEFAULT_COMPSS_VERSION}

    --dir [app dir]     Directory in which the app and the required files are
                        stored. MUST BE A RELATIVE PATH. Default: "."
    --wd [working dir]  Directory in which to copy the application files in the
                        image. REMEMBER that, if this flag is used to change from
                        the default value, then the new working directory must
                        be set in the project.xml and / or resources.xml files.
                        Default: /compss
    --exclude [dir,...] List of files and / or directories to exclude from the
                        image creation. Must follow format file1,file2,...,fileN.
                        All mentioned files / directories can use wildcards as
                        stated in:
                        https://docs.docker.com/engine/reference/builder/#dockerignore-file
    --python |          Using either of these flags will make the script create a
    --pycompss          pycompss app image instead of a regular Java one.
EOF
}

ARCH=${DEFAULT_ARCH}
WORKER_TYPE=${DEFAULT_WORKER_TYPE}
APP_DIR=${DEFAULT_APP_DIR}
APP_WD=${DEFAULT_WD}
BASE_TYPE=${DEFAULT_BASE_TYPE}
while [ "$1" != "" ]; do
    case $1 in
        -h | --help)
            usage
            exit;;
        --name)
            IMAGE_NAME="$2"
            shift;;
        --base)
            BASE_IMAGE_NAME="$2"
            shift;;
        --version)
            COMPSS_VERSION="$2"
            shift;;
        --dir)
            APP_DIR="$2"
            shift;;
        --wd)
            APP_WD="$2"
            shift;;
        --exclude)
            EXCLUDED_FILES="$2"
            shift;;
        --python | --pycompss)
            BASE_TYPE=pycompss3;;
        *)
        error "The flag $1 is unknown. Check out for typos!"
        exit;;
    esac
    shift
done

if [ -z "$IMAGE_NAME" ]; then
    error "The name of the image to be created must be specified"
    exit 1
fi

if [ -z "$BASE_IMAGE_NAME" ]; then
    warn "No base image specified."
    if [ -z "$COMPSS_VERSION" ]; then
        warn "No version specified. Falling back to default base image: ${BASE_TYPE}/${WORKER_TYPE}-worker-${ARCH}:${DEFAULT_COMPSS_VERSION}"
        BASE_IMAGE_NAME="${BASE_TYPE}/${WORKER_TYPE}-worker-${ARCH}:${DEFAULT_COMPSS_VERSION}"
    else
        BASE_IMAGE_NAME="${BASE_TYPE}/${WORKER_TYPE}-worker-${ARCH}:${COMPSS_VERSION}"
    fi
fi

DOCKERIGNORE_EXISTED=false
if [ -f ".dockerignore" ]; then
    DOCKERIGNORE_EXISTED=true
    echo ".dockerfile" >> .dockerignore
else
    echo ".dockerfile" > .dockerignore
fi

if [ -n "$EXCLUDED_FILES" ]; then
    AMOUNT=$(charamount $EXCLUDED_FILES ",")
    for i in `seq 1 $(($AMOUNT + 1))`
    do
        echo $(echo "$EXCLUDED_FILES" | cut -d "," -f"$i") >> .dockerignore
    done
fi

DOCKERFILE=$(cat <<EOF
FROM ${BASE_IMAGE_NAME}
COPY ${APP_DIR} ${APP_WD}
EOF
)
mkdir .dockerfile
echo "$DOCKERFILE" > .dockerfile/Dockerfile
docker build -t "${IMAGE_NAME}" -f .dockerfile/Dockerfile .
rm -rf .dockerfile
if [ "$DOCKERIGNORE_EXISTED" = "false" ]; then
    rm .dockerignore
fi
