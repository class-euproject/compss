ARG COMPSS_VERSION=2.5.rc1907

FROM compss/nio-worker-amd64:${COMPSS_VERSION}
LABEL maintainer="unai.perez@bsc.es"

COPY compss /opt/COMPSs
RUN apk add --update --no-cache python3 && ln -s /usr/bin/python3 /usr/bin/python