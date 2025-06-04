# Replicated log

In all iterations except 0, **master node** will be hosted on `localhost:5000`, and **secondary nodes** will be hosted on `localhost:500*`
> \* from 1 to 3

## Test Iteration 0

`python .\iteration0\echo_server.py`

`python .\iteration0\echo_client.py`

## Test Iteration *

`cd iteration*`

`docker-compose up --build -d`
