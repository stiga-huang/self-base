#!/bin/bash

TITAN_HOME=/home/hadoop/titan-bin-0.5.4
# 为使HADOOP_CLASSPATH中将我们的log4j.properties放在最前，又把titan依赖的jar包放在最后，需要事先处理HADOOP_CLASSPATH
export HADOOP_CLASSPATH=`pwd`:`hadoop classpath`:"$TITAN_HOME/lib/*"

JAR_PATH=base-test-1.0-SNAPSHOT.jar
MAIN_CLASS=cn.edu.pku.hql.titan.mapreduce.VertexLoaderMR
HDFS_INPUT_PATH=huangql/data/input/people/allPeopleData
LOCAL_TITAN_CONF=vertex_only_titan.properties

hadoop jar $JAR_PATH $MAIN_CLASS "$LOCAL_TITAN_CONF" "$HDFS_INPUT_PATH"
