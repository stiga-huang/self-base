#!/bin/bash
#
# author:  huangquanlong
# comment: run MapReduce test
#

TITAN_HOME=/home/hadoop/titan-bin-0.5.4
# 为使HADOOP_CLASSPATH中将我们的log4j.properties放在最前，又把titan依赖的jar包放在最后，需要事先处理HADOOP_CLASSPATH
export HADOOP_CLASSPATH=`pwd`:`hadoop classpath`:"$TITAN_HOME/lib/*"
#export HADOOP_USER_CLASSPATH_FIRST=true # important!

JAR_PATH=base-test-1.0-SNAPSHOT.jar
MAIN_CLASS=cn.edu.pku.hql.titan.mapreduce.RawLoaderMR2
HDFS_INPUT_PATH=huangql/data/input/people/allPeopleData
LOCAL_TITAN_CONF=vertex_only_titan.properties
EDGE_TIMES=1

# test of raw dataloader on small set of edges
#hadoop jar /home/workspace/titan-0.5.4-hadoop2/lib/hbase-test-1.0-SNAPSHOT.jar com.mininglamp.titan.mapreduce.RawLoaderMR raw-titan.properties rycc_relation 0,19,9 DistributedDataloader/data/rycc/relationFileNames true

# test of raw dataloader on large set of edges
hadoop jar $JAR_PATH $MAIN_CLASS $LOCAL_TITAN_CONF rycc_relation 0,19,9 huangql/data/input_small/edgeFileNames $EDGE_TIMES
