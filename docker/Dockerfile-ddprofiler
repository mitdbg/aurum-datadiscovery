# ddprofiler
FROM ubuntu:18.04


# The 'apt-add-repository' command comes from the software-properties-common
# package, which will install ~50MB of stuff just to be able to edit a text
# file.  Here, we install the package, add the repository, and then purge the
# package all in none step to avoid the overhead in the final container.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        software-properties-common \
    && \
    add-apt-repository ppa:webupd8team/java && \
    apt-get purge -y --auto-remove \
        software-properties-common \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add the JDK 8 and accept licenses (mandatory)
RUN echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
    echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections

# Install Java 8
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        oracle-java8-installer \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /var/cache/oracle-jdk8-installer

COPY . /aurum

#RUN cd /aurum/ddprofiler && ./gradlew clean fatJar
RUN cd /aurum/ddprofiler && \
    bash build.sh && \
    rm -f /aurum/ddprofiler/build/distributions/*

WORKDIR /aurum/ddprofiler

ENTRYPOINT ["/bin/bash", "/aurum/ddprofiler/run.sh", "${*}"]
