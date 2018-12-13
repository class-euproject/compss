FROM ubuntu:16.04
MAINTAINER COMPSs Support <support-compss@bsc.es>

# =============================================================================
# Configuration required to use the image for jenkins testing
# =============================================================================

# Install Essentials
RUN apt-get update && \
# Install Packages
    apt-get install -y \
	git  \
    vim \
    wget \
    openssh-server \
    sudo && \
# Enable ssh to localhost
    yes yes | ssh-keygen -f /root/.ssh/id_rsa -t rsa -N '' > /dev/null && \
    cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys && \
# Enable repo compression
    git config --global core.compression 9 && \
# =============================================================================
# Dependencies for building COMPSs
# =============================================================================
    apt-get update && \
# Build dependencies
    apt-get -y install maven && \
# Runtime dependencies
    apt-get -y install openjdk-8-jdk graphviz xdg-utils && \
# Bindings-common-dependencies
    apt-get -y install libtool automake build-essential && \
# Python-binding dependencies
    apt-get -y install python-dev libpython2.7 python-pip python3-pip
RUN pip2 install numpy==1.15.4 dill guppy decorator && \
    pip3 install numpy==1.15.4 dill decorator  && \
# Python-redis dependencies
    pip2 install redis redis-py-cluster && \
    pip3 install redis redis-py-cluster && \
# pycompsslib dependencies
    pip2 install scipy==1.0.0 scikit-learn==0.19.1 pandas==0.23.1 matplotlib==2.2.3 flake8 codecov coverage psutil && \
    pip3 install scipy==1.0.0 scikit-learn==0.19.1 pandas==0.23.1 matplotlib==2.2.3 flake8 codecov coverage psutil && \
# C-binding dependencies
    apt-get -y install libboost-all-dev libxml2-dev csh && \
# Extrae dependencies
    apt-get -y install libxml2 gfortran libpapi-dev papi-tools && \
# Misc. dependencies
    apt-get update && \
    apt-get -y install openmpi-bin openmpi-doc libopenmpi-dev uuid-runtime curl bc && \
    yes jenkins2017 | passwd && \
# AutoParallel dependencies
    apt-get -y install libgmp3-dev flex bison libbison-dev texinfo libffi-dev && \
    pip2 install astor sympy enum34 islpy && \
# Configure user environment
# =============================================================================
# System configuration
# =============================================================================
# Add environment variables
    echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> /etc/environment && \
    echo "MPI_HOME=/usr/lib64/openmpi" >> /etc/environment && \
    echo "LD_LIBRARY_PATH=/usr/lib64/openmpi/lib" >> /etc/environment && \
    mkdir /run/sshd

# Copy framework files for installation and testing
COPY . /framework

# Install COMPSs
RUN cd /framework && \
    ./submodules_get.sh && \
    ./submodules_patch.sh && \
    sudo -E /framework/builders/buildlocal /opt/COMPSs && \
    cp /framework/utils/docker/entrypoint.sh / && \
    rm -r /framework

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$JAVA_HOME/jre/lib/amd64/server

# Expose SSH port and run SSHD
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]

# If container is run in interactive, entrypoint runs the sshd server
ENTRYPOINT ["/entrypoint.sh"]
