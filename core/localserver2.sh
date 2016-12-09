#!/bin/bash
nohup java -server -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport conf/datanode2-1.cnf >& s1 &
nohup java -server -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport conf/datanode2-2.cnf >& s2 &
nohup java -server -cp target/kdb-1.0-SNAPSHOT.jar kdb.NettyTransport conf/datanode2-3.cnf >& s3 &
