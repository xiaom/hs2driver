# hs2driver

A C++ Query Submitter for HiveServer2

What is it?
===========

`hs2driver` is a tool that submitting queries to HiveServer2 with compressor
enabled and consuming results.

Dependencies
============

`hs2driver` requires gcc 4.8+, cmake 2.8+, thrift 0.9.0 and boost 1.54+. The
instrution of building thrift library is describe in [the thrift
website](https://thrift.apache.org/docs/install/). `hs2driver` is also shipped
with a Dockerfile which encapluse all dependencies for quickly testing. See the
section [Run with Docker Containers](#Run with Docker Containers) for more
details.

Installation
============

`hs2driver` is tested against Ubuntu 14.04.2 LTS.  To build it, under the root
of the project directory, run the following commands:

```bash
mkdir build
cmake ..
make
```

How to use
==========

The program generated is called `qh`, which is short for query submitter for HiveServer2.

```
Usage: qh [--host  host-name] [--port port-name] [--compressor compressor-name] --query query-string

Options:
  --help                     Help on args options
  --host arg (=localhost)    HiveServer2 Hostname/IP Address
  --port arg (=10000)        HiveServer2 Port Number
  --compressor arg (=PIN)    Compressor name
  --query arg (=show tables) Queries, seperated by semi-colon
```


Run with Docker Containers
==========================

The Dockerfile requires docker 1.50+ and docker-compose 1.1.0+. You can find
detailed instructions on [the docker
website](https://docs.docker.com/compose/install/).

To build the docker image, under the root directory of current project, run:

```sh
$ docker-compose build # build the images
```

It will build a docker image called `hs2driver_client`.


To run a docker container and use the query submitter,  just add
`docker-compose run client` before `qh` command:

```sh
$ docker-compose run client qh [arguments]
```

**Notes.** If you run hiveserver2 container on your localhost in OS X, the
hiveserver2-ip should be the ip of boot2docker vm.  You can use `boot2docker
ip` to get it.

