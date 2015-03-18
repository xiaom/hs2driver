# hs2driver

A C++ Query Submitter for HiveServer2




Run with docker containers
--------------------------

The docker container providing here has all dependencies you need to compile
the program Assume that the latest version of [docker] and [docker-compose] are
installed.

In the *root directory of current project*, we can do the following:

```sh
$ docker-compose build # build the images
$ docker-compose up    # start the container
```

It will start a container named `hs2driver_c1` for you.

To  run the query submitter:

```sh
$ docker exec hs2driver_d ./qh --host [hiveserver2-ip] --query "show tables; select * from Integer_table"
```

You can login into the container as a normal 'VM' using

```sh
$ docker exec -it hs2driver bash
```


References
----------

- [Boost::ProgramOptions](http://www.boost.org/doc/libs/1_57_0/doc/html/program_options/tutorial.html)

[docker](https://docs.docker.com/installation/) 
[docker-compose](https://docs.docker.com/compose/)


