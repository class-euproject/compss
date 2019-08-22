FROM docker:dind
LABEL maintainer="unai.perez@bsc.es"

ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk

COPY . /root/framework

RUN echo "@edgetesting http://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories && \
	apk add --no-cache --update git nano wget openssh-server openssh-client maven openjdk8 graphviz xdg-utils libtool autoconf automake build-base boost-dev libxml2-dev tcsh util-linux curl bc libffi-dev gradle bash openmpi@edgetesting openmpi-dev@edgetesting lxd@edgetesting && \
	ssh-keygen -A && \
	ssh-keygen -f /root/.ssh/id_rsa -t rsa -N '' && \
	cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
	echo 'root:`mkpasswd randompassword`' | chpasswd && \
	ln -s /usr/local/bin/docker /usr/bin/docker

RUN cd /root/framework && \
	./submodules_get.sh && \
	./submodules_patch.sh

ENTRYPOINT ["/bin/sh", "-c"]
CMD ["nohup /usr/sbin/sshd -D & dockerd-entrypoint.sh"]
