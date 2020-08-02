#!/bin/sh

# start sshd service
/etc/init.d/ssh start

# start hdfs
/root/hadoop/sbin/start-dfs.sh

# start yarn
/root/hadoop/sbin/start-yarn.sh

/bin/bash