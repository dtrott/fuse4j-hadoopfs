#!/bin/sh
#
# Note if you move mount.hadoop into your path and then add an entry like this to /etc/fstab:
#       /hadoop                 hdfs://localhost:9000		hadoop  user 0 0
# 
# You can mount with:  mount /hadoop


./mount.hadoop /hadoop hdfs://localhost:9000
