#!/bin/bash


lodex=./lodex

while IFS=',' read -r key value
do
  $lodex get $key 
done < $1
