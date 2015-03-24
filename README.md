# hs2driver

A C++ Query Submitter for HiveServer2

# What is it?

`hs2driver` provide a command-line tool that submits queries to HiveServer2 with compressor
enabled and consumes query results.

It can be used for testing compress techniques for [real-time ResultSet compression protocol](https://github.com/simbatech/hive-slick/wiki/%5BJIRA%5D-Proposal).

For how to develop a decompressor, please see the file called [TUTORIAL.md](TUTORIAL.md).

# Dependencies

`hs2driver` requires gcc 4.8+, cmake 2.8+, boost 1.54+, and thrift 0.9.0. 

For gcc, cmake and boost, you can easily install them via package management systems in Linux/OS X platforms. The instrution of building thrift library is described in [the thrift
website](https://thrift.apache.org/docs/install/). 

`hs2driver` is also shipped
with a Dockerfile, which encapluse all dependencies for quickly testing. See the
section [Run with Docker Containers](#run-with-docker-containers) for more
details.

# Build from Source

`hs2driver` has been tested against Ubuntu 14.04.2 LTS and OS X Yosemite 10.10.2.

To build it, under the root of the project directory, run the following commands:

```bash
mkdir build
cmake ..
make
```

How to use
==========

The command generated is called `qh`, which is short for **q**uery submitter for **H**iveServer2.

```
Usage: qh [--host  host-name] [--port port-name] [--compressor compressor-name] --query query-string

Options:
  --help                     Help on args options
  --host arg (=localhost)    HiveServer2 Hostname/IP Address
  --port arg (=10000)        HiveServer2 Port Number
  --compressor arg (=PIN)    Compressor name
  --query arg (=show tables) Queries, seperated by semi-colon
```


## Run with Docker Containers

The Dockerfile requires docker 1.50+ and docker-compose 1.1.0+. You can find
detailed instructions on [the docker
website](https://docs.docker.com/compose/install/).

To build the docker image, under the root directory of current project, run:

```sh
$ docker-compose build # build the images
```

It will build a docker image called `hs2driver_client`.


To use the query submitter,  just add `docker-compose run client` before `qh` command:

```sh
$ docker-compose run client qh [arguments]
```

**Notes.** If you run HiveServer2 container in OS X with boot2docker. The
HiveServer2 IP address should be the ip of boot2docker vm, which can be obtained by running `boot2docker
ip`.
