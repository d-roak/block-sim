#!/bin/sh

docker build -t block-sim .
docker run -i -t -d block-sim
