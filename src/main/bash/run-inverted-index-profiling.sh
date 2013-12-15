#!/bin/bash

INPUT=input/
OUTPUT=output/inverted-index-profiling/
JAR="../../../target/MapReduceExamples-1.0-SNAPSHOT.jar"
PACKAGE="com.hakunamapdata.examples"

hadoop fs -rmr $OUTPUT
hadoop fs -rmr $INPUT
hadoop fs -mkdir $INPUT

hadoop fs -put ../../../src/main/resources/hadoop $INPUT

hadoop jar $JAR "${PACKAGE}.InvertedIndex" $INPUT/hadoop "$OUTPUT/ii" true
