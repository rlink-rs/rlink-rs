#!/bin/bash

EXECUTOR_WORKSPACE=$(cd $(dirname $0); pwd)

nohup $EXECUTOR_WORKSPACE/rlink-standalone type=TaskManager \
  1>$EXECUTOR_WORKSPACE/log/stdout \
  2>$EXECUTOR_WORKSPACE/log/stderr \
  & echo $!>$EXECUTOR_WORKSPACE/pid
