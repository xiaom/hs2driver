FROM xiaom/thrift-boost:0.90

ADD . /code
RUN mkdir -p /code/build
WORKDIR /code/build
RUN cd /code/build && cmake .. && make && mv /code/compressorInfo.json /code/build
