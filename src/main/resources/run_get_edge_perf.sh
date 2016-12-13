#!/bin/bash

CLASSPATH="$TITAN_HOME/lib/*:base-test-1.0-SNAPSHOT.jar"
JVM_OPTION="-Xms1g -Xmx1g"
MAIN_CLASS=cn.edu.pku.hql.titan.GetEdgePerf

RAW_TITAN_CONF=raw_titan.properties
SCOPA_TITAN_CONF=scopa_titan.properties

LOW_DEGREE_VID_FILE=/home/huangql/ndbc/testCases/ryccMidTest/vids/vids_low_100
SAMPLE_VID_FILE=/home/huangql/ndbc/testCases/ryccMidTest/vids/vids_sample_100
HIGH_DEGREE_VID_FILE=/home/huangql/ndbc/testCases/ryccMidTest/vids/vids_high_100

REPEAT=5
RES_DIR=./get_edges_perf_logs
OUTPUT_LOG=$RES_DIR/$(date +"%Y%m%d-%H%M%S").log

if [ ! -d "$RES_DIR" ]; then
	mkdir "$RES_DIR"
fi

echo logging in $OUTPUT_LOG

printInfo(){
	echo $* | tee -a $OUTPUT_LOG
}

printInfo JVM_OPTIONS: $JVM_OPTION
printInfo

for file in "$LOW_DEGREE_VID_FILE" "$SAMPLE_VID_FILE" "$HIGH_DEGREE_VID_FILE"; do
	printInfo vid_file: $file
	#echo vid_file: $file | tee $OUTPUT_LOG
	for mode in both_raw_first both_scopa_first; do
		printInfo "-----------------------------------------------"
		for i in `seq $REPEAT`; do
			ARGS="$RAW_TITAN_CONF $SCOPA_TITAN_CONF $LOW_DEGREE_VID_FILE $mode"
			java -cp "$CLASSPATH" $JVM_OPTION $MAIN_CLASS $ARGS | tee -a $OUTPUT_LOG
			printInfo
		done
	done
	printInfo
done

