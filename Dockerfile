# Use phusion/baseimage as base image. To make your builds reproducible, make
# sure you lock down to a specific version, not to `latest`!
# See https://github.com/phusion/baseimage-docker/blob/master/Changelog.md for
# a list of version numbers.
FROM phusion/baseimage:0.9.16

CMD ["/sbin/my_init"]
# Install build requirements
RUN apt-get update
RUN apt-get install --no-install-recommends -y wget curl \
      build-essential g++ git cmake zlib1g-dev \
      libboost-dev libboost-test-dev libboost-program-options-dev \
      libboost-system-dev libboost-filesystem-dev \
      libevent-dev automake libtool flex bison pkg-config libssl-dev \
      libboost-regex-dev libboost-date-time-dev libboost-thread-dev libboost-chrono-dev
      
ENV THRIFT_VERSION 0.9.0
RUN curl -SsfLO "https://archive.apache.org/dist/thrift/$THRIFT_VERSION/thrift-$THRIFT_VERSION.tar.gz" && \
    tar -xzvf thrift-$THRIFT_VERSION.tar.gz && \
    mv thrift-$THRIFT_VERSION thrift
    
RUN cd thrift && \
    ./configure --without-tests --without-java --without-python && make -j 8 && make install
ADD . /code
RUN mkdir -p /code/bin
WORKDIR /code/bin
RUN cmake .. ; make; mv /code/compressorInfo.json /code/bin
