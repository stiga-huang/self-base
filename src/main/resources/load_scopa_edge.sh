#!/bin/bash
#
# author:  huangquanlong
# comment: run MapReduce test
#

# 为使HADOOP_CLASSPATH中将我们的log4j.properties放在最前，又把titan依赖的jar包放在最后，需要事先处理HADOOP_CLASSPATH
export JAVA_HOME=/usr/java/default
export TITAN_HOME=/home/hadoop/titan-bin-0.5.4
export HBASE_LIB_DIR=/home/hadoop/hbase-1.0.1.1/lib
export HBASE_CONF_DIR=/home/hadoop/hbase-1.0.1.1/conf

export JAR_FILE=/home/hadoop/huangql/ndbc/base-test-1.0-SNAPSHOT.jar
export LOCAL_TITAN_CONF=/home/hadoop/huangql/ndbc/scopa_titan.properties
export HDFS_NAMES_FILE=huangql/data/input/railway_relation/edgeFileNames
export LINE_PER_SPLIT=4
export MAIN_CLASS=cn.edu.pku.hql.titan.mapreduce.ScopaLoaderMR

export HADOOP_CLASSPATH=`pwd`:`hadoop classpath`:"$HBASE_CONF_DIR":"$TITAN_HOME/lib/*"
export HADOOP_USER_CLASSPATH_FIRST=true # important!

echo HADOOP_CLASSPATH=
hadoop classpath

print_error() {
    echo -e "\e[1;31m`date` -- $1\e[0m"  # print message in red color
}

print_info() {
    echo -e "\e[1;32m`date` -- $1\e[0m"  # print message in green color
}

#test of scopa dataloader on small set of edges
#echo test of scopa dataloader on small set of edges
#hadoop jar /home/workspace/titan-0.5.4-hadoop2/lib/hbase-test-1.0-SNAPSHOT.jar com.mininglamp.titan.mapreduce.ScopaLoaderMR titan-hbase-es.properties rycc_relation 0,19,9 DistributedDataloader/data/rycc/relationFileNames2

HDFS_OUTPUT_PATH=/tmp/scopaBulkLoading
hdfs dfs -rm -r $HDFS_OUTPUT_PATH > /dev/null 2>&1

# test of scopa dataloader on large set of edges
print_info "test of scopa dataloader on large set of edges"
hadoop jar $JAR_FILE $MAIN_CLASS $LOCAL_TITAN_CONF rycc_relation 0,19,9 $HDFS_NAMES_FILE $LINE_PER_SPLIT

if [ $? != 0 ]; then
    print_error 'batch process failed'
    exit 1
fi

export HADOOP_USER_NAME=hdfs
hdfs dfs -chown -R hbase:hbase $HDFS_OUTPUT_PATH

print_info "completing bulkload, moving HFile into HBase"
hadoop jar $HBASE_LIB_DIR/hbase-server-1.0.1.1.jar completebulkload $HDFS_OUTPUT_PATH rycc_relation
if [ $? != 0 ]; then
    print_error "failed to complete bulkload"
    exit 1
fi

print_info "finished bulk load"
hdfs dfs -rm -r $HDFS_OUTPUT_PATH > /dev/null 2>&1
