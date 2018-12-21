#!/bin/bash


lodex=./lodex

while IFS=',' read -r key value
do
  $lodex put $key $value
done < $1
