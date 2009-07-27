ls #!/bin/sh

LAUNCHER=${HOME}/workspace/fuse4j/native/javafs
MOUNT_POINT=/hadoop
FS_CLASS=fuse4j/hadoopfs/FuseHdfsClient
M2_REPO=${HOME}/.m2/repository

JAVA_JVM_VERSION=1.6
FUSE4J_VER=2.4.0.0-SNAPSHOT
HADOOP_VER=0.20.0
HADOOPFS_VER=1.0.0-SNAPSHOT

CLASSPATH=""
CLASSPATH="$CLASSPATH:$M2_REPO/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar"
CLASSPATH="$CLASSPATH:$M2_REPO/log4j/log4j/1.2.13/log4j-1.2.13.jar"
CLASSPATH="$CLASSPATH:$M2_REPO/fuse4j/fuse4j-core/${FUSE4J_VER}/fuse4j-core-${FUSE4J_VER}.jar"
CLASSPATH="$CLASSPATH:$M2_REPO/fuse4j/fuse4j-hadoopfs/${HADOOPFS_VER}/fuse4j-hadoopfs-${HADOOPFS_VER}.jar"
CLASSPATH="$CLASSPATH:$M2_REPO/org/apache/hadoop/hadoop-core/${HADOOP_VER}/hadoop-core-${HADOOP_VER}.jar"

export JAVA_JVM_VERSION
export CLASSPATH

$LAUNCHER -C${FS_CLASS} "-J-Djava.class.path=$CLASSPATH" $MOUNT_POINT -f
