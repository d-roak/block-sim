#!/bin/sh

# use one less core than the maximum
num_cores=$(expr $(nproc --all) - 1)
protos=(btc eth)
sims=5

echo "Cores: $num_cores"
echo "Protocols: ${protos[*]}"
echo "Simulations number: $sims (each proto)"
echo "----------------------"

# Should be disabled if server is shared with other users
echo "Stopping running containers"
docker stop $(docker ps -q) &> /dev/null

echo "Building docker image"
docker build -t block-sim . > /dev/null

for p in ${protos[*]}
do
  for i in $(seq 1 $sims)
  do
    docker run -i -t -e "proto=$p" -e "sim=$i" --name $p-$i -d block-sim > /dev/null
    lines=$(docker ps -q | wc -l)
    echo "Running $p-$i ($lines/$num_cores)"
    while [ $lines -ge $num_cores ]
    do
      sleep 5
      lines=$(docker ps -q | wc -l)
    done
  done
done

while [ ! -z "$(docker ps -q)" ]
do
  sleep 5
done
echo "All done"

# cleanup
echo "Cleaning up"
for p in btc eth prop
do
  for i in {1..10}
  do
    docker rm $p-$i &> /dev/null
  done
done
