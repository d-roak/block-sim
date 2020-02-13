#!/bin/bash

set +e

for p in btc eth prop
do
  for i in {1..10}
  do
    pypy3 $p.py conf_$p $i
  done
done
