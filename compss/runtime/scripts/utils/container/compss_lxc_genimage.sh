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
DEFAULT_COMPSS_VERSION=2.5.rc1907
DEFAULT_APP_DIR=.
DEFAULT_WD=/compss
DEFAULT_FILE_LIST="*"

usage() {
    cat <<EOF
Script for the creation of a COMPSs application LXC image. Available flags:

Mandatory flags:
    --name [name]       Name to give to the created image.

Optional flags:
    --base [image]      Full name of the base image to be used.
                Format: [<repository>/][<username>/]<image name>[:<tag>]
        or
    --version [version] Version of COMPSs to use. If this is specified, the name
                        of the base image will automatically fall back to:
                        ${DEFAULT_BASE_TYPE}-\$VERSION

            If neither are used, the used image will automatically fall back to:
              ${DEFAULT_BASE_TYPE}-${DEFAULT_COMPSS_VERSION}

    --wd [working dir]  Directory in which to copy the application files in the
                        image. REMEMBER that, if this flag is used to change from
                        the default value, then the new working directory must
                        be set in the project.xml and / or resources.xml files.
                        Default: /compss

    --cp                Comma-delimited list of files to copy into the working
                        directory of the image. Wildcards such as "*" can be
                        used. If the flag is not set, all the contents of the
                        current directory will be copied.

    --python |          Using either of these flags will make the script create a
    --pycompss          pycompss app image instead of a regular Java one.
EOF
}

ARCH=${DEFAULT_ARCH}
APP_WD=${DEFAULT_WD}
BASE_TYPE=${DEFAULT_BASE_TYPE}
FILE_LIST=${DEFAULT_FILE_LIST}
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
        --wd)
            APP_WD="$2"
            shift;;
        --cp)
            FILE_LIST="$2"
            shift;;
        --python | --pycompss)
            BASE_TYPE=pycompss;;
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
        warn "No version specified. Falling back to default base image: ${BASE_TYPE}-${DEFAULT_COMPSS_VERSION}"
        BASE_IMAGE_NAME="${BASE_TYPE}-${DEFAULT_COMPSS_VERSION}"
    else
        BASE_IMAGE_NAME="${BASE_TYPE}-${COMPSS_VERSION}"
    fi
fi

LXC=`command -v lxc`
if [ -z ${LXC} ]; then
    error "LXC not found"
fi
${LXC} launch ${BASE_IMAGE_NAME} base
${LXC} exec base mkdir /compss
for FILE in `echo ${FILE_LIST} | sed -E 's/,/ /'`; do
    ${LXC} file push -pr ${FILE} base/compss/
done
${LXC} stop base
${LXC} publish base --alias ${IMAGE_NAME}
${LXC} delete base