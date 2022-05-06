# Q-A-web-service (qaws)

<p align="right">
  <a href="https://docs.python.org/3.9/">
    <img src="https://img.shields.io/badge/Python-3.9-FFE873.svg?labelColor=4B8BBE"
        alt="Python requirement">
  </a>
</p>

## About

A simple web service application:
* takes POST requests in form `{"questions_num": integer}`;
* sends a request to a public [API](https://jservice.io/api/random?count=1) to get specified early
number of questions (`"questions_num"`);
* saves part information of received questions to own DB (in case the question is found in the DB,
the application sends new requests to the public API to get unique questions);
* responds to the request by returning previously saved question or, if there is none, an empty 
  object (in my case empty dictionary`{}`).

Tags: \
`Flask`, `PostgreSQL`, `psycopg2`, `Docker`

## Docker instructions

Before starting, [install Compose](https://docs.docker.com/compose/install/) if you do not have 
it. \
Clone this git repository.

`Dockerfile` starts with 
[Python 3.9 parent image](https://hub.docker.com/r/library/python/tags/3.9) and describes 
further modifications.
`docker-compose.yml` describes the services that makes `qaws-app`. See the 
[`docker-compose.yml` reference](https://docs.docker.com/compose/compose-file/) for more information.

To build and run Docker image of the `qaws-app` run the following from the project 
root directory: 

```shell
$ docker compose up
```

To shut down running services and clean up use either of these methods:
* typing `Ctrl-C`
* or switch to a different shell and run (also from the project root directory)

```shell
$ docker compose down
```

To connect to DB switch to a different shell and run:
```shell
$ psql -U postgres -W -h 127.0.0.1 -p 5432 postgres
```

Input password: "postgres". It is assumed you have psql - PostgreSQL interactive terminal.

To send POST request to the `qaws-app` use:
```shell
$ curl -X POST http://127.0.0.1:8000/ \
        -H 'Content-Type: application/json' \
        -d '{"questions_num":<your integer number>}'
```
for example:
```
$ curl -X POST http://127.0.0.1:8000/ \
        -H 'Content-Type: application/json' \
        -d '{"questions_num":1}'
```
or run script located in the project root directory:
```shell
$ ./request.sh <your integer number>
```
