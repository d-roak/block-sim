#!/bin/sh

# use one less core than the maximum
num_cores=3

for p in btc eth prop
do
  for i in {1..10}
  do
    docker build -t block-sim-$p .
    docker rm $p-$i
    docker run -i -t -e "proto=$p" -e "sim=$i" --name $p-$i -d block-sim-$p
    lines=$(docker ps -q | wc -l)
    while [ $lines -ge $num_cores ]
    do
        sleep 5
    done
  done
done