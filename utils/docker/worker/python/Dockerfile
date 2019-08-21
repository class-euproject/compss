FROM alpine:3.8
LABEL maintainer="unai.perez@bsc.es"

COPY compss /opt/COMPSs
RUN apk add --update --no-cache util-linux openjdk8-jre-base python3 && \
	ln -s /usr/bin/python3 /usr/bin/python
