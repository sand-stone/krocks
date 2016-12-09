#!/bin/bash -x
nohup java -server -mx20g -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport $1 &
