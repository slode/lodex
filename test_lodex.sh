#!/bin/bash

lodex=./lodex

assertEqual()
{
  if [ -z "$2" ]
  then
    echo "ERROR: missing arguments"
    exit
  fi


  if [ ! "$1" = "$2" ]
  then
    echo "not ok:  \"$1 == $2\""
  else
    echo "ok: \"$1 == $2\""
  fi
}

rm -f database.ldx database2.ldx
rm -f database.ldx.idx database2.ldx.idx

echo "Testing lodex database CLI"

$lodex put "{\"_id\": \"testkey\", \"value\": \"testvalue\"}"
assertEqual `$lodex get "{\"_id\": \"testkey\"}" ` testvalue

$lodex get {"_id": "testkey"} > /dev/null
assertEqual $? 0

$lodex get invalid_testkey 2> /dev/null
assertEqual $? 1
assertEqual `$lodex stats | grep items | cut -f2` 1
assertEqual "`$lodex dump --sep=':' | tr '\n' ','`" "{\"_id\": \"testkey\", \"value\": \"testvalue\"},"

$lodex put another_testkey testvalue
assertEqual $? 0
assertEqual `$lodex stats | grep items | cut -f2` 2
assertEqual "`$lodex dump --sep=':' | tr '\n' ','`" "{\"_id\": \"another_testkey\", \"value\": \"testvalue\"},{\"_id\": \"testkey\", \"value\": \"testvalue\"},"

$lodex delete another_testkey
assertEqual $? 0
assertEqual `$lodex stats | grep items | cut -f2` 1
assertEqual "`$lodex dump --sep=':' | tr '\n' ','`" "{\"_id\": \"testkey\", \"value\": \"testvalue\"},"
$lodex get another_testkey 2> /dev/null
assertEqual $? 1

$lodex get testkey --db=database2.ldx 2> /dev/null
assertEqual $? 1

$lodex dump | $lodex load --db=database2.ldx
assertEqual "`$lodex dump --db=database2.ldx --sep=':' | tr '\n' ','`" "{\"_id\": \"testkey\", \"value\": \"testvalue\"},"

