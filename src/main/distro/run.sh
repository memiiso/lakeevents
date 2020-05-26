#!/bin/bash

if [ -z "$JAVA_HOME" ]; then
  JAVA_BINARY="java"
else
  JAVA_BINARY="$JAVA_HOME/bin/java"
fi

RUNNER=$(ls lakeevents-*runner.jar)

exec $JAVA_BINARY $DEBEZIUM_OPTS $JAVA_OPTS -cp "$RUNNER:conf:lib/*" io.pprobi.lakeevents.Main
